//! The crate's layers, read from its sources with code behind every cfg.
//!
//! The check reads paths and imports, not macro bodies or name resolution.
//! It follows each import and re-export to the module that defines the item.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use syn::visit::{self, Visit};
use syn::{Attribute, Item, ItemMod, ItemUse, UseTree, Visibility};

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
    in_block: bool,
}

/// Returns `true` if `attrs` hold a cfg that needs `test`.
fn test_only(attrs: &[Attribute]) -> bool {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg"))
        .any(|attr| {
            let cfg = attr
                .meta
                .require_list()
                .map_or(String::new(), |list| list.tokens.to_string());
            cfg.contains("test") && !cfg.contains("not (test")
        })
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
    fn name(&mut self, path: Vec<String>, line: usize, reexport: bool) {
        self.krate.named.push(Named {
            from: self.module.clone(),
            path,
            test: self.test,
            reexport,
            at: format!("{}:{line}", self.file.display()),
        });
    }

    fn imports(&mut self, prefix: &mut Vec<String>, tree: &UseTree, item: &ItemUse) {
        let (path, alias) = match tree {
            UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                self.imports(prefix, &path.tree, item);
                prefix.pop();
                return;
            }
            UseTree::Group(group) => {
                group
                    .items
                    .iter()
                    .for_each(|tree| self.imports(prefix, tree, item));
                return;
            }
            UseTree::Name(name) if name.ident == "self" => (prefix.clone(), prefix.last().cloned()),
            UseTree::Name(name) => (
                [prefix.as_slice(), &[name.ident.to_string()]].concat(),
                Some(name.ident.to_string()),
            ),
            UseTree::Rename(rename) => (
                [prefix.as_slice(), &[rename.ident.to_string()]].concat(),
                Some(rename.rename.to_string()),
            ),
            UseTree::Glob(_) => (prefix.clone(), None),
        };
        if let (Some(alias), false) = (alias, self.in_block) {
            self.krate
                .imports
                .entry((self.module.clone(), alias))
                .or_insert(path.clone());
        }
        let reexport = !matches!(item.vis, Visibility::Inherited);
        self.name(path, item.use_token.span.start().line, reexport);
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
            self.name(parts, line, false);
        }
        visit::visit_path(self, path);
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        self.imports(&mut Vec::new(), &item.tree, item);
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

    fn visit_block(&mut self, block: &'ast syn::Block) {
        let in_block = std::mem::replace(&mut self.in_block, true);
        visit::visit_block(self, block);
        self.in_block = in_block;
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
        in_block: false,
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
            let Some((module, through)) = self
                .resolve(path.clone())
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
