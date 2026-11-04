//! The crate's layers, read from its sources with code behind every cfg.
//!
//! The check reads paths, imports at any depth, and the paths in macro
//! bodies, not name resolution. It follows each import and re-export to the
//! module that defines the item, and takes a glob import as naming its module.

use proc_macro2::{Ident, TokenStream, TokenTree};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use syn::punctuated::Punctuated;
use syn::visit::{self, Visit};
use syn::{Attribute, ImplItem, Item, ItemMod, ItemUse, Meta, Token, UseTree, Visibility};

/// The layers, lowest first, as the crate's doc lists them.
const LAYERS: [&str; 8] = [
    "seam", "rt", "tio", "data", "session", "proxy", "device", "firmware",
];

type Module = Vec<String>;

/// A path the code names, from the crate root.
struct Named {
    from: Module,
    path: Vec<String>,
    test: bool,
    /// Whether it is a re-export, which is not an edge of a parent to its own
    /// children.
    reexport: bool,
    /// Whether it is a glob import, which names the module it imports from.
    glob: bool,
    at: String,
}

/// The modules of a crate, what each module's top-level imports name, and
/// every path its code names.
#[derive(Default)]
struct Crate {
    modules: BTreeSet<Module>,
    imports: BTreeMap<(Module, String), Vec<String>>,
    named: Vec<Named>,
}

/// A walk through the items of `module`, which `file` holds.
struct Walk<'a> {
    krate: &'a mut Crate,
    file: PathBuf,
    directory: PathBuf,
    module: Module,
    test: bool,
    /// What the imports of each block the walk is in name, by alias,
    /// innermost last.
    scopes: Vec<BTreeMap<String, Vec<String>>>,
}

/// Returns `true` if `attrs` hold a cfg that needs `test`.
fn test_only(attrs: &[Attribute]) -> bool {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg"))
        .filter_map(|attr| attr.parse_args::<Meta>().ok())
        .any(|cfg| without_test(&cfg) == Some(false))
}

/// Whether `cfg` holds in a build without `test`, or `None` when that turns
/// on an option the build may or may not set.
fn without_test(cfg: &Meta) -> Option<bool> {
    let Meta::List(list) = cfg else {
        return match cfg.path().is_ident("test") {
            true => Some(false),
            false => None,
        };
    };
    let args = list
        .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        .ok()?;
    let values: Vec<Option<bool>> = args.iter().map(without_test).collect();
    match list.path.get_ident()?.to_string().as_str() {
        "not" => values.first().copied().flatten().map(|value| !value),
        "all" if values.contains(&Some(false)) => Some(false),
        "all" => values
            .iter()
            .all(|value| *value == Some(true))
            .then_some(true),
        "any" if values.contains(&Some(true)) => Some(true),
        "any" => values
            .iter()
            .all(|value| *value == Some(false))
            .then_some(false),
        _ => None,
    }
}

/// Returns `true` if `module` is `parent` or lies inside it.
fn under(module: &Module, parent: &[&str]) -> bool {
    module.len() >= parent.len() && module.iter().zip(parent).all(|(part, name)| part == name)
}

/// Returns the position of `module`'s layer in [`LAYERS`], or one past the
/// last for the crate root.
fn layer(module: &Module) -> usize {
    match module.first() {
        Some(top) => LAYERS
            .iter()
            .position(|layer| top == layer)
            .unwrap_or_else(|| panic!("give {top} a layer")),
        None => LAYERS.len(),
    }
}

fn shown(module: &Module) -> String {
    match module.is_empty() {
        true => "the crate root".into(),
        false => module.join("::"),
    }
}

impl Walk<'_> {
    /// Records `path`, its first part read through the imports of the blocks
    /// the walk is in.
    fn name(&mut self, mut path: Vec<String>, line: usize, reexport: bool, glob: bool) {
        let imported = self
            .scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(&path[0]))
            .filter(|imported| imported[0] != path[0]);
        if let Some(imported) = imported {
            path = [imported.as_slice(), &path[1..]].concat();
        }
        self.krate.named.push(Named {
            from: self.module.clone(),
            path,
            test: self.test,
            reexport,
            glob,
            at: format!("{}:{line}", self.file.display()),
        });
    }

    /// Records each path a macro's tokens spell out, as `a::b`, at any depth
    /// of brackets.
    fn scan(&mut self, tokens: TokenStream) {
        let mut path: Vec<Ident> = Vec::new();
        let mut colons = 0;
        for tree in tokens {
            match tree {
                TokenTree::Ident(ident) => {
                    if colons != 2 {
                        self.spelled(&mut path);
                    }
                    path.push(ident);
                    colons = 0;
                }
                TokenTree::Punct(punct) if punct.as_char() == ':' && colons < 2 => colons += 1,
                TokenTree::Group(group) => {
                    self.spelled(&mut path);
                    colons = 0;
                    self.scan(group.stream());
                }
                TokenTree::Punct(_) | TokenTree::Literal(_) => {
                    self.spelled(&mut path);
                    colons = 0;
                }
            }
        }
        self.spelled(&mut path);
    }

    /// Records the path of `parts` a macro spelled, if it has more than one,
    /// and clears it.
    fn spelled(&mut self, parts: &mut Vec<Ident>) {
        if parts.len() > 1 {
            let line = parts[0].span().start().line;
            let path = parts.iter().map(Ident::to_string).collect();
            self.name(path, line, false, false);
        }
        parts.clear();
    }

    /// Takes the imports of `item` into the block or module the walk is in,
    /// and records each path it names when `named`.
    fn imports(&mut self, item: &ItemUse, named: bool) {
        let mut found = Vec::new();
        imported(&mut Vec::new(), &item.tree, &mut found);
        let line = item.use_token.span.start().line;
        let reexport = !matches!(item.vis, Visibility::Inherited);
        for (path, alias) in found {
            let glob = alias.is_none();
            match (alias, self.scopes.last_mut()) {
                (Some(alias), Some(scope)) => {
                    scope.entry(alias).or_insert(path.clone());
                }
                (Some(alias), None) => {
                    self.krate
                        .imports
                        .entry((self.module.clone(), alias))
                        .or_insert(path.clone());
                }
                (None, _) => {}
            }
            if named {
                self.name(path, line, reexport, glob);
            }
        }
    }
}

/// Each import `tree` makes after `prefix`, the path it names and the alias
/// it binds, of which a glob binds none.
fn imported(
    prefix: &mut Vec<String>,
    tree: &UseTree,
    found: &mut Vec<(Vec<String>, Option<String>)>,
) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            imported(prefix, &path.tree, found);
            prefix.pop();
        }
        UseTree::Group(group) => group
            .items
            .iter()
            .for_each(|tree| imported(prefix, tree, found)),
        UseTree::Name(name) if name.ident == "self" => {
            found.push((prefix.clone(), prefix.last().cloned()))
        }
        UseTree::Name(name) => found.push((
            [prefix.as_slice(), &[name.ident.to_string()]].concat(),
            Some(name.ident.to_string()),
        )),
        UseTree::Rename(rename) => found.push((
            [prefix.as_slice(), &[rename.ident.to_string()]].concat(),
            Some(rename.rename.to_string()),
        )),
        UseTree::Glob(_) => found.push((prefix.clone(), None)),
    }
}

impl<'ast> Visit<'ast> for Walk<'_> {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        let parts: Vec<String> = path
            .segments
            .iter()
            .map(|part| part.ident.to_string())
            .collect();
        let line = path
            .segments
            .first()
            .map_or(0, |part| part.ident.span().start().line);
        if parts.len() > 1 {
            self.name(parts, line, false, false);
        }
        visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.scan(mac.tokens.clone());
        visit::visit_macro(self, mac);
    }

    fn visit_impl_item(&mut self, item: &'ast ImplItem) {
        let attrs: &[Attribute] = match item {
            ImplItem::Fn(item) => &item.attrs,
            ImplItem::Const(item) => &item.attrs,
            ImplItem::Type(item) => &item.attrs,
            _ => &[],
        };
        let test = self.test;
        self.test |= test_only(attrs);
        visit::visit_impl_item(self, item);
        self.test = test;
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        self.imports(item, true);
    }

    fn visit_item(&mut self, item: &'ast Item) {
        let attrs: &[Attribute] = match item {
            Item::Fn(item) => &item.attrs,
            Item::Impl(item) => &item.attrs,
            Item::Struct(item) => &item.attrs,
            Item::Enum(item) => &item.attrs,
            Item::Const(item) => &item.attrs,
            Item::Static(item) => &item.attrs,
            Item::Type(item) => &item.attrs,
            Item::Trait(item) => &item.attrs,
            Item::Use(item) => &item.attrs,
            _ => &[],
        };
        let test = self.test;
        self.test |= test_only(attrs);
        visit::visit_item(self, item);
        self.test = test;
    }

    /// A block's imports hold in all of it, so they are read before its code.
    fn visit_block(&mut self, block: &'ast syn::Block) {
        self.scopes.push(BTreeMap::new());
        for stmt in &block.stmts {
            if let syn::Stmt::Item(Item::Use(item)) = stmt {
                self.imports(item, false);
            }
        }
        visit::visit_block(self, block);
        self.scopes.pop();
    }

    fn visit_item_mod(&mut self, module: &'ast ItemMod) {
        assert!(
            !module.attrs.iter().any(|attr| attr.path().is_ident("path")),
            "teach the architecture check to resolve #[path] modules before adding one"
        );
        let name = module.ident.to_string();
        let mut path = self.module.clone();
        path.push(name.clone());
        let test = self.test || name == "tests" || test_only(&module.attrs);
        let directory = self.directory.join(&name);
        let (file, items) = match &module.content {
            Some((_, items)) => (self.file.clone(), items.clone()),
            None => {
                let file = directory.join("mod.rs");
                let file = if file.exists() {
                    file
                } else {
                    self.directory.join(format!("{name}.rs"))
                };
                let code = fs::read_to_string(&file)
                    .unwrap_or_else(|error| panic!("{}: {error}", file.display()));
                let parsed = syn::parse_file(&code)
                    .unwrap_or_else(|error| panic!("{}: {error}", file.display()));
                (file, parsed.items)
            }
        };
        walk(self.krate, file, directory, path, &items, test);
    }
}

/// Reads `items`, the items of `module` in `file`, and every module they declare.
fn walk(
    krate: &mut Crate,
    file: PathBuf,
    directory: PathBuf,
    module: Module,
    items: &[Item],
    test: bool,
) {
    krate.modules.insert(module.clone());
    let mut walk = Walk {
        krate,
        file,
        directory,
        module,
        test,
        scopes: Vec::new(),
    };
    items.iter().for_each(|item| walk.visit_item(item));
}

impl Crate {
    /// Reads the crate whose root is `lib`.
    fn read(lib: &Path) -> Crate {
        let mut krate = Crate::default();
        let file = syn::parse_file(&fs::read_to_string(lib).unwrap()).unwrap();
        walk(
            &mut krate,
            lib.into(),
            lib.parent().unwrap().into(),
            Vec::new(),
            &file.items,
            false,
        );
        krate
    }

    /// Returns `path`, named in `module`, from the crate root, or `None` for
    /// a path outside the crate. A path may start at a child module or at a
    /// module `module` imports.
    fn absolute(&self, module: &Module, path: &[String]) -> Option<Vec<String>> {
        let (first, rest) = path.split_first()?;
        let mut child = module.clone();
        child.push(first.clone());
        let (base, rest) = match first.as_str() {
            "crate" => (Vec::new(), rest),
            "self" => (module.clone(), rest),
            "super" => {
                let supers = 1 + rest.iter().take_while(|part| *part == "super").count();
                (
                    module[..module.len().checked_sub(supers)?].to_vec(),
                    &path[supers..],
                )
            }
            _ if self.modules.contains(&child) => (child, rest),
            _ => {
                let imported = self
                    .imports
                    .get(&(module.clone(), first.clone()))
                    .filter(|imported| imported[0] != *first)?;
                let imported = self
                    .absolute(module, imported)
                    .filter(|imported| self.modules.contains(imported))?;
                (imported, rest)
            }
        };
        Some([base.as_slice(), rest].concat())
    }

    /// Returns the module that defines the item `path` names, and each
    /// module whose import or re-export the path went through on the way.
    /// Returns `None` for a path that names a module.
    fn resolve(&self, mut path: Vec<String>) -> Option<(Module, Vec<Module>)> {
        let mut through = Vec::new();
        loop {
            let module = (0..=path.len())
                .rev()
                .map(|n| path[..n].to_vec())
                .find(|m| self.modules.contains(m))?;
            let name = path.get(module.len())?;
            let target = self.imports.get(&(module.clone(), name.clone()));
            match target.and_then(|target| self.absolute(&module, target)) {
                Some(target) if through.len() < 16 => {
                    path = [target.as_slice(), &path[module.len() + 1..]].concat();
                    through.push(module);
                }
                Some(_) | None => return Some((module, through)),
            }
        }
    }

    /// Returns each breach of the layers, as where it is and what it breaks.
    fn violations(&self) -> Vec<String> {
        let mut found = Vec::new();
        let mut edges = BTreeMap::new();
        for named in &self.named {
            let Some(path) = self.absolute(&named.from, &named.path) else {
                continue;
            };
            let globbed =
                (named.glob && self.modules.contains(&path)).then(|| (path.clone(), vec![]));
            let Some((module, through)) = globbed
                .or_else(|| self.resolve(path.clone()))
                .filter(|(module, _)| !module.is_empty())
            else {
                continue;
            };
            let what = format!(
                "{}: {} names {}",
                named.at,
                shown(&named.from),
                path.join("::")
            );
            let (from, to) = (layer(&named.from), layer(&module));
            let allowed = match (LAYERS.get(from).copied(), LAYERS[to]) {
                _ if from == to => true,
                (Some("data"), "tio") => under(&module, &["tio", "packet"]),
                (Some("device"), "proxy") if !named.test => {
                    under(&named.from, &["device", "runtime"])
                }
                (Some("proxy"), "device") => under(&named.from, &["proxy", "tests", "turmoil"]),
                _ => to < from,
            };
            if !allowed {
                found.push(format!("{what}, which reaches up to {}", shown(&module)));
            }
            let seam = matches!(path.first().map(String::as_str), Some("chan" | "net"));
            let above = |over: &&Module| layer(over) > to && !(over.is_empty() && seam);
            if let Some(over) = through
                .iter()
                .find(above)
                .filter(|_| !named.from.is_empty())
            {
                found.push(format!(
                    "{what} through {}, above {}",
                    shown(over),
                    shown(&module)
                ));
            }
            if !named.test && module.iter().any(|part| part == "tests") {
                found.push(format!("{what}, a test module, from production code"));
            }
            let common = named
                .from
                .iter()
                .zip(&module)
                .take_while(|(a, b)| a == b)
                .count();
            let node = |module: &Module| {
                module
                    .get(common)
                    .cloned()
                    .unwrap_or_else(|| "(self)".into())
            };
            let own = named.reexport && common == named.from.len();
            if !named.test && !own && node(&named.from) != node(&module) {
                let key = (
                    named.from[..common].to_vec(),
                    node(&named.from),
                    node(&module),
                );
                edges.entry(key).or_insert(what);
            }
        }
        found.extend(cycles(&edges));
        found
    }
}

/// Returns each edge that lies on a cycle among the children of one parent.
fn cycles(edges: &BTreeMap<(Module, String, String), String>) -> Vec<String> {
    let next = |parent: &Module, node: &String| -> Vec<String> {
        edges
            .keys()
            .filter(|(p, a, _)| p == parent && a == node)
            .map(|(_, _, b)| b.clone())
            .collect()
    };
    let reaches = |parent: &Module, start: &String, goal: &String| {
        let (mut seen, mut stack) = (BTreeSet::new(), vec![start.clone()]);
        while let Some(node) = stack.pop() {
            if &node == goal {
                return true;
            }
            if seen.insert(node.clone()) {
                stack.extend(next(parent, &node));
            }
        }
        false
    };
    edges
        .iter()
        .filter(|((parent, from, to), _)| reaches(parent, to, from))
        .map(|((parent, from, to), at)| {
            format!(
                "{at}, so {from} -> {to} lies on a cycle in {}",
                shown(parent)
            )
        })
        .collect()
}

#[test]
fn the_layers_hold_and_siblings_form_a_dag() {
    let lib = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
    let violations = Crate::read(&lib).violations();
    assert!(
        violations.is_empty(),
        "the layers do not hold\n{}",
        violations.join("\n")
    );
}

/// The breaches of a crate whose root holds `code` in `tio`, beside a
/// `proxy` above it and a `tests` module inside it.
fn breaches(code: &str) -> Vec<String> {
    let source =
        format!("mod proxy {{ pub struct X; }} mod tio {{ {code} mod tests {{ pub struct X; }} }}");
    let file = syn::parse_file(&source).unwrap();
    let mut krate = Crate::default();
    walk(
        &mut krate,
        "lib.rs".into(),
        ".".into(),
        Vec::new(),
        &file.items,
        false,
    );
    krate.violations()
}

/// Each way code can name a higher layer, or a test module from production
/// code, is a breach, and the lawful forms beside them are not.
#[test]
fn each_way_of_naming_what_a_module_may_not_is_found() {
    let breaking = [
        "fn f() { crate::proxy::X::f(); }",
        "use crate::proxy::*;",
        "fn f() { use crate::proxy as p; p::X::f(); }",
        "fn f() { use crate::proxy::X; let _ = X; }",
        "fn f() { println!(\"{:?}\", crate::proxy::X::f()); }",
        "#[cfg(not(all(test, feature = \"x\")))] fn f() { crate::tio::tests::X::f(); }",
        "#[cfg(any(feature = \"serial\", test))] fn f() { crate::tio::tests::X::f(); }",
    ];
    for code in breaking {
        assert_ne!(breaches(code), Vec::<String>::new(), "{code}");
    }
    let lawful = [
        "",
        "#[cfg(test)] fn f() { crate::tio::tests::X::f(); }",
        "#[cfg(all(test, feature = \"x\"))] fn f() { crate::tio::tests::X::f(); }",
        "struct S; impl S { #[cfg(test)] fn f() { crate::tio::tests::X::f(); } }",
        "fn f() { let a: u8 = 1; println!(\"{a}\"); }",
    ];
    for code in lawful {
        assert_eq!(breaches(code), Vec::<String>::new(), "{code}");
    }
}
