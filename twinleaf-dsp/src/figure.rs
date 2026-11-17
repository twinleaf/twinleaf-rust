//! Makes a PDF or an SVG of a figure written in Typst.
//!
//! There are two ways to it. With the `typst` feature the Typst compiler is
//! part of this crate and the figure is compiled here, with fonts that are
//! built in. Without it, a `typst` on the PATH is run. Either way the
//! packages the figure imports are found as the Typst command finds them.
//! That is in a directory given for them, in the user's package
//! directories, and failing those from Typst Universe, which is the one
//! thing here that reaches for the network.

use std::path::{Path, PathBuf};

/// What to make of a figure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Pdf,
    Svg,
}

impl Format {
    /// The name Typst knows the format by, which is also its extension.
    pub fn name(self) -> &'static str {
        match self {
            Self::Pdf => "pdf",
            Self::Svg => "svg",
        }
    }
}

/// Why a figure was not made.
#[derive(Debug, thiserror::Error)]
pub enum FigureError {
    /// Typst read the figure and would not have it. Each entry is one
    /// thing it said.
    #[error("Typst could not compile the figure:\n{}", .0.join("\n"))]
    Compile(Vec<String>),
    #[error(
        "there is no `typst` to run: install Typst, or build with `--features typst`, \
         or take the source with `--typst` and compile it elsewhere"
    )]
    NoTypst,
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// Compiles `source`, a whole Typst document, and returns the file made of
/// it. `packages` is a directory to look for packages in before anywhere
/// else, laid out as Typst lays them out, which is
/// `preview/<name>/<version>`.
pub fn render(
    source: &str,
    format: Format,
    packages: Option<&Path>,
) -> Result<Vec<u8>, FigureError> {
    #[cfg(feature = "typst")]
    {
        embedded::render(source, format, packages)
    }
    #[cfg(not(feature = "typst"))]
    {
        installed(source, format, packages)
    }
}

/// Whether figures are compiled in this process.
pub const EMBEDDED: bool = cfg!(feature = "typst");

/// Has the `typst` on the PATH compile the figure.
#[cfg_attr(feature = "typst", allow(dead_code))]
fn installed(
    source: &str,
    format: Format,
    packages: Option<&Path>,
) -> Result<Vec<u8>, FigureError> {
    use std::process::Command;

    // Typst reads a file and writes a file. Both go in a directory of
    // their own, which is gone again when this returns.
    let scratch = Scratch::new()?;
    let input = scratch.0.join("figure.typ");
    let output = scratch.0.join(format!("figure.{}", format.name()));
    std::fs::write(&input, source)?;

    let mut command = Command::new("typst");
    command.arg("compile").arg("--format").arg(format.name());
    if let Some(packages) = packages {
        command.arg("--package-path").arg(packages);
    }
    let ran = command.arg(&input).arg(&output).output();
    let ran = match ran {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(FigureError::NoTypst)
        }
        other => other?,
    };
    if !ran.status.success() {
        let said = String::from_utf8_lossy(&ran.stderr);
        return Err(FigureError::Compile(vec![said.trim_end().to_owned()]));
    }
    Ok(std::fs::read(&output)?)
}

/// A directory of this process's own, removed when it is dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> std::io::Result<Self> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.subsec_nanos());
        let path =
            std::env::temp_dir().join(format!("twinleaf-dsp-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(feature = "typst")]
mod embedded {
    use super::{FigureError, Format};
    use std::path::Path;
    use typst::diag::{FileError, FileResult, SourceDiagnostic};
    use typst::foundations::{Bytes, Datetime, Duration};
    use typst::layout::Abs;
    use typst::syntax::{FileId, RootedPath, Source, VirtualPath, VirtualRoot};
    use typst::text::{Font, FontBook};
    use typst::utils::LazyHash;
    use typst::{Library, LibraryExt, World, WorldExt};
    use typst_kit::downloader::SystemDownloader;
    use typst_kit::files::{FileLoader, FileStore};
    use typst_kit::fonts::FontStore;
    use typst_kit::packages::{FsPackages, SystemPackages, UniversePackages};
    use typst_layout::PagedDocument;

    /// What this program calls itself when it asks Typst Universe for a
    /// package.
    const AGENT: &str = concat!("twinleaf-dsp/", env!("CARGO_PKG_VERSION"));

    /// Serves the one document there is, and the packages it imports.
    struct Loader {
        main: FileId,
        text: String,
        packages: SystemPackages,
    }

    impl FileLoader for Loader {
        fn load(&self, id: FileId) -> FileResult<Bytes> {
            match id.root() {
                VirtualRoot::Project if id == self.main => {
                    Ok(Bytes::from_string(self.text.clone()))
                }
                // A figure is one file. It reads no others.
                VirtualRoot::Project => {
                    Err(FileError::NotFound(id.vpath().get_without_slash().into()))
                }
                VirtualRoot::Package(spec) => self.packages.obtain(spec)?.load(id.vpath()),
            }
        }
    }

    struct Figure {
        library: LazyHash<Library>,
        fonts: FontStore,
        files: FileStore<Loader>,
        main: FileId,
    }

    impl World for Figure {
        fn library(&self) -> &LazyHash<Library> {
            &self.library
        }

        fn book(&self) -> &LazyHash<FontBook> {
            self.fonts.book()
        }

        fn main(&self) -> FileId {
            self.main
        }

        fn source(&self, id: FileId) -> FileResult<Source> {
            self.files.source(id)
        }

        fn file(&self, id: FileId) -> FileResult<Bytes> {
            self.files.file(id)
        }

        fn font(&self, index: usize) -> Option<Font> {
            self.fonts.font(index)
        }

        fn today(&self, _offset: Option<Duration>) -> Option<Datetime> {
            // A figure of a graph has no date in it, and one that asked
            // for today's would not come out the same tomorrow.
            None
        }
    }

    pub fn render(
        source: &str,
        format: Format,
        packages: Option<&Path>,
    ) -> Result<Vec<u8>, FigureError> {
        let path = VirtualPath::new("figure.typ").expect("a plain file name is a path");
        let main = RootedPath::new(VirtualRoot::Project, path).intern();
        // The directory given comes first, then where the user keeps
        // packages, then the cache, which is filled from Typst Universe.
        let universe = UniversePackages::new(SystemDownloader::new(AGENT));
        let data = packages
            .map(FsPackages::new)
            .or_else(FsPackages::system_data);
        let packages = SystemPackages::from_parts(data, FsPackages::system_cache(), universe);

        let mut fonts = FontStore::new();
        fonts.extend(typst_kit::fonts::embedded());
        let world = Figure {
            library: LazyHash::new(Library::default()),
            fonts,
            files: FileStore::new(Loader {
                main,
                text: source.to_owned(),
                packages,
            }),
            main,
        };

        let said = |diagnostics: &[SourceDiagnostic]| -> Vec<String> {
            diagnostics.iter().map(|d| describe(&world, d)).collect()
        };
        let compiled = typst::compile::<PagedDocument>(&world);
        let document = compiled
            .output
            .map_err(|errors| FigureError::Compile(said(errors.as_slice())))?;
        match format {
            Format::Pdf => typst_pdf::pdf(&document, &typst_pdf::PdfOptions::default())
                .map_err(|errors| FigureError::Compile(said(errors.as_slice()))),
            Format::Svg => {
                let options = typst_svg::SvgOptions::default();
                Ok(typst_svg::svg_merged(&document, &options, Abs::zero()).into_bytes())
            }
        }
    }

    /// One thing Typst said, with the line it said it of where that line
    /// is in the figure itself and not in a package.
    fn describe(world: &Figure, diagnostic: &SourceDiagnostic) -> String {
        let mut text = diagnostic.message.to_string();
        let within = diagnostic.span.id() == Some(world.main);
        if let Some(range) = world.range(diagnostic.span).filter(|_| within) {
            if let Ok(source) = world.source(world.main) {
                let before = &source.text()[..range.start.min(source.text().len())];
                let line = before.matches('\n').count() + 1;
                text = format!("line {line}: {text}");
            }
        }
        for hint in &diagnostic.hints {
            text.push_str(&format!("\n  hint: {}", hint.v));
        }
        text
    }
}
