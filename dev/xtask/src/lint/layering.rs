//! Checks the one-way dependencies between the layers under `crates/`:
//! `api`, `shared`, `ledger`, `compute` and `enroute`.
//!
//! `api` is the wire contract and `shared` what the engine's roles share.
//! `ledger` and `compute` are the two roles, and `enroute` (one crate,
//! the root package) is the service built on all of them. `api` names only
//! itself, `shared` names itself and `api`, `ledger` names `shared`, `compute`
//! names `shared` and `api`, and `enroute` names all five. An `api` crate has
//! an end outside Rust, which a build here does not show: the gap surfaces
//! when somebody generates a client in another language. Cargo accepts any
//! acyclic graph, so nothing but this rule stops a crate reaching sideways or
//! upward; a crate's layer is its directory, so moving a crate changes what
//! it may depend on. The ledger and the compute never name each other: they
//! meet only at the `Ledger` service in `enroute-shared`, so each can run as a
//! node of its own. Inside `enroute-compute`, [`check_modules`] does the same
//! for the top modules, from the `crate::` paths in each file's code.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use toml::Value;
use tree_sitter::Node;

use super::Violation;
use super::manifest::{DEPENDENCY_TABLES, Manifests, package_name};

/// The source directory of the crate that [`MODULE_RULES`] bind.
const COMPUTE_SRC: &str = "crates/compute/src";

/// The one-way dependencies between the top modules of `enroute-compute`.
const MODULE_RULES: [ModuleRule; 4] = [
    ModuleRule {
        modules: Modules::Only(&["git"]),
        may_name: Modules::Only(&["sys"]),
        why: "git formats are the base that everything else reads, on only the FFI under them",
    },
    ModuleRule {
        modules: Modules::Only(&["storage"]),
        may_name: Modules::Only(&["git", "sys"]),
        why: "layer files know git formats and the OS, and nothing built on them",
    },
    ModuleRule {
        modules: Modules::Only(&["repo"]),
        may_name: Modules::AllBut(&["api", "compute", "hooks", "http", "maintenance", "query"]),
        why: "the node, maintenance and requests read views, so views cannot reach back up",
    },
    ModuleRule {
        modules: Modules::AllBut(&["api", "http"]),
        may_name: Modules::AllBut(&["api", "http"]),
        why: "the two ports are the top: nothing else serves through them",
    },
];

/// A set of top modules: those listed, or all others.
#[derive(Clone, Copy)]
enum Modules {
    Only(&'static [&'static str]),
    AllBut(&'static [&'static str]),
}

impl Modules {
    fn contains(self, module: &str) -> bool {
        match self {
            Self::Only(list) => list.contains(&module),
            Self::AllBut(list) => !list.contains(&module),
        }
    }
}

struct ModuleRule {
    modules: Modules,
    may_name: Modules,
    why: &'static str,
}

pub(crate) fn check(manifests: &Manifests) -> Vec<Violation> {
    let mut violations = Vec::new();

    // A directory under `crates/` naming no layer would be waved through
    // unconstrained — the same hole this rule exists to close, reached by a
    // typo'd or unregistered directory rather than by a dependency edge.
    for (file, _) in &manifests.members {
        if let Some(dir) = layer_dir(file)
            && layer(dir).is_none()
        {
            violations.push(Violation {
                file: file.clone(),
                line: None,
                message: format!(
                    "`crates/{dir}` names no layer: a crate under `crates/` belongs to one of {}",
                    Layer::ALL.map(Layer::name).join(", ")
                ),
            });
        }
    }

    let owner: HashMap<&str, Layer> = manifests
        .members
        .iter()
        .filter_map(|(file, manifest)| {
            Some((
                package_name(manifest)?,
                layer_of(file, &manifests.root_dir)?,
            ))
        })
        .collect();

    for (file, manifest) in &manifests.members {
        // `dev/` tooling drives every layer by design, so it sits outside the
        // rule rather than being an exception inside it.
        let Some(from) = layer_of(file, &manifests.root_dir) else {
            continue;
        };
        for (dep, to) in reaches_too_far(manifest, from, &owner) {
            violations.push(Violation {
                file: file.clone(),
                line: None,
                message: format!(
                    "`{dep}` is in the `{}` layer, and a crate in `{}` may name only {}",
                    to.name(),
                    from.name(),
                    from.allowed()
                        .iter()
                        .map(|layer| format!("`{}`", layer.name()))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
    }

    violations
}

/// Every `crate::` path in the compute's code that a module rule forbids.
///
/// A name after `crate::` that is no module is a root re-export, which hides
/// the module that the file depends on.
pub(crate) fn check_modules(sources: &[(PathBuf, String)], root: &Path) -> Vec<Violation> {
    let src = root.join(COMPUTE_SRC);
    let files: Vec<(&Path, Option<&str>, &str)> = sources
        .iter()
        .filter_map(|(file, source)| {
            let relative = file.strip_prefix(&src).ok()?;
            Some((file.as_path(), top_module(relative), source.as_str()))
        })
        .collect();
    // Else a moved crate would turn the rule off without a word.
    if files.is_empty() {
        return vec![Violation {
            file: src,
            line: None,
            message: "no Rust files: the module rules bind `enroute-compute` here".to_string(),
        }];
    }
    let modules: BTreeSet<&str> = files.iter().filter_map(|(_, top, _)| *top).collect();

    let mut parser = match super::rust_parser() {
        Ok(parser) => parser,
        Err(violation) => return vec![violation],
    };
    let mut violations = Vec::new();
    for (file, from, source) in files {
        // The crate root declares the modules and re-exports them.
        let Some(from) = from else { continue };
        let Some(tree) = parser.parse(source.as_bytes(), None) else {
            continue;
        };
        let mut named = Vec::new();
        crate_paths(tree.root_node(), source, &mut named);
        for (line, to) in named {
            let message = if !modules.contains(to) {
                format!(
                    "`crate::{to}` is a root re-export: inside `enroute-compute`, name the module that owns it"
                )
            } else if let Some(rule) = MODULE_RULES.iter().find(|rule| {
                to != from && rule.modules.contains(from) && !rule.may_name.contains(to)
            }) {
                format!("`{from}` may not name `crate::{to}`: {}", rule.why)
            } else {
                continue;
            };
            violations.push(Violation {
                file: file.to_path_buf(),
                line: Some(line),
                message,
            });
        }
    }
    violations
}

/// The top module of a file under the compute's `src`; `None` for the
/// crate root.
fn top_module(relative: &Path) -> Option<&str> {
    let first = relative.components().next()?.as_os_str().to_str()?;
    let top = first.strip_suffix(".rs").unwrap_or(first);
    (top != "lib").then_some(top)
}

/// Every top module that a `crate::` path under `node` names, with its line.
///
/// Comments, doc comments and strings are nodes of their own, so a doc link
/// is not a dependency.
fn crate_paths<'a>(node: Node<'_>, source: &'a str, out: &mut Vec<(usize, &'a str)>) {
    if node.kind() == "crate"
        && let Some(parent) = node.parent()
        && parent.child_by_field_name("path") == Some(node)
    {
        let line = node.start_position().row + 1;
        if let Some(name) = parent.child_by_field_name("name") {
            out.extend(text(name, source).map(|name| (line, name)));
        } else if let Some(list) = parent.child_by_field_name("list") {
            let mut cursor = list.walk();
            out.extend(
                list.named_children(&mut cursor)
                    .filter_map(first_segment)
                    .filter_map(|first| Some((line, text(first, source)?))),
            );
        }
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        crate_paths(child, source, out);
    }
}

/// The first segment of an item in a `use` list, such as `git` in
/// `git::oid::Oid` or `git::{oid, pack}`.
fn first_segment(node: Node<'_>) -> Option<Node<'_>> {
    match node.kind() {
        "identifier" => Some(node),
        "scoped_identifier" | "scoped_use_list" | "use_as_clause" => {
            first_segment(node.child_by_field_name("path")?)
        }
        _ => None,
    }
}

fn text<'a>(node: Node<'_>, source: &'a str) -> Option<&'a str> {
    node.utf8_text(source.as_bytes()).ok()
}

/// The name of every dependency `manifest` declares, in any table.
fn dependencies(manifest: &Value) -> impl Iterator<Item = &str> {
    DEPENDENCY_TABLES
        .iter()
        .filter_map(|table| manifest.get(*table).and_then(Value::as_table))
        .flat_map(toml::Table::keys)
        .map(String::as_str)
}

/// Every workspace dependency of `manifest` a crate in `from` may not name,
/// with the layer it belongs to.
///
/// A dependency outside the workspace answers to no layer and is skipped.
fn reaches_too_far<'a>(
    manifest: &'a Value,
    from: Layer,
    owner: &HashMap<&'a str, Layer>,
) -> Vec<(&'a str, Layer)> {
    dependencies(manifest)
        .filter_map(|dep| {
            let to = owner.get(dep).copied()?;
            (!from.may_name(to)).then_some((dep, to))
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layer {
    Api,
    Shared,
    Ledger,
    Compute,
    Enroute,
}

impl Layer {
    const ALL: [Self; 5] = [
        Self::Api,
        Self::Shared,
        Self::Ledger,
        Self::Compute,
        Self::Enroute,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Shared => "shared",
            Self::Ledger => "ledger",
            Self::Compute => "compute",
            Self::Enroute => "enroute",
        }
    }

    /// Every layer a crate in this one may name, itself included.
    fn allowed(self) -> &'static [Self] {
        match self {
            Self::Api => &[Self::Api],
            Self::Shared => &[Self::Api, Self::Shared],
            Self::Ledger => &[Self::Shared, Self::Ledger],
            Self::Compute => &[Self::Api, Self::Shared, Self::Compute],
            Self::Enroute => &Self::ALL,
        }
    }

    fn may_name(self, other: Self) -> bool {
        self.allowed().contains(&other)
    }
}

fn layer(dir: &str) -> Option<Layer> {
    Layer::ALL.into_iter().find(|layer| layer.name() == dir)
}

/// The layer a member belongs to: the root package is `enroute`, and a member
/// outside `crates/` has none.
fn layer_of(manifest_path: &Path, root_dir: &Path) -> Option<Layer> {
    if manifest_path.parent() == Some(root_dir) {
        return Some(Layer::Enroute);
    }
    layer_dir(manifest_path).and_then(layer)
}

/// The directory naming a member's layer, or `None` outside `crates/`.
///
/// `crates/<layer>/<crate>/Cargo.toml`, or `crates/<layer>/Cargo.toml` for a
/// layer of one crate. `dev/` tooling answers to no layer.
fn layer_dir(manifest_path: &Path) -> Option<&str> {
    let dir = manifest_path.parent()?;
    let layer = if dir.parent()?.file_name()? == "crates" {
        dir
    } else {
        dir.parent()?
    };
    if layer.parent()?.file_name()? != "crates" {
        return None;
    }
    layer.file_name()?.to_str()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use tempfile::{TempDir, tempdir};

    use super::{COMPUTE_SRC, check, check_modules};
    use crate::lint::manifest;

    /// The modules that the module-rule tests name, each with an empty file.
    const MODULES: [&str; 8] = [
        "api/mod.rs",
        "compute.rs",
        "git/mod.rs",
        "hooks.rs",
        "http.rs",
        "query.rs",
        "storage/mod.rs",
        "sys.rs",
    ];

    /// The module-rule messages for `(path under src, source)` files of
    /// the compute, besides [`MODULES`].
    fn module_violations(files: &[(&str, &str)]) -> Vec<String> {
        let root = Path::new("/ws");
        let src = root.join(COMPUTE_SRC);
        let sources: Vec<(PathBuf, String)> = MODULES
            .iter()
            .map(|path| (*path, ""))
            .chain(files.iter().copied())
            .map(|(path, source)| (src.join(path), source.to_string()))
            .collect();
        check_modules(&sources, root)
            .into_iter()
            .map(|violation| violation.message)
            .collect()
    }

    /// Asserts that `files` break the module rules once, as `expected` says.
    fn assert_one(files: &[(&str, &str)], expected: &str) {
        let found = module_violations(files);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains(expected), "{found:?}");
    }

    // Everything reads git formats, so they may not read anything back.
    #[test]
    fn git_names_only_sys() {
        assert_one(
            &[(
                "git/pack.rs",
                "use crate::git::oid::Oid;\nuse crate::storage::Layer;\n",
            )],
            "`git` may not name `crate::storage`",
        );
    }

    // A grouped `use` names each of its modules.
    #[test]
    fn storage_names_only_git_and_sys() {
        assert_one(
            &[(
                "storage/layer.rs",
                "use crate::{git::oid::Oid, sys, query::Error};\n",
            )],
            "`storage` may not name `crate::query`",
        );
    }

    // A rustdoc link is no dependency; a path in code outside `use` is.
    #[test]
    fn doc_links_do_not_count_and_code_paths_do() {
        assert_one(
            &[(
                "storage/store.rs",
                "//! See [`crate::compute`].\n/// [`crate::hooks`]\nfn f() { crate::query::run(); }\n",
            )],
            "`storage` may not name `crate::query`",
        );
    }

    #[test]
    fn only_the_ports_name_the_ports() {
        assert_one(
            &[
                ("http.rs", "use crate::api::router;\n"),
                ("compute.rs", "use crate::http::git_router;\n"),
            ],
            "`compute` may not name `crate::http`",
        );
    }

    // The layers above read views, so views may not read them.
    #[test]
    fn repo_may_not_reach_up() {
        assert_one(
            &[(
                "repo/mod.rs",
                "use crate::storage::Layer;\nuse crate::compute::ComputeConfig;\n",
            )],
            "`repo` may not name `crate::compute`",
        );
    }

    // A root re-export would hide the module that a file depends on.
    #[test]
    fn a_root_re_export_is_flagged() {
        assert_one(
            &[("query.rs", "fn f(_: &crate::Compute) {}\n")],
            "`crate::Compute` is a root re-export",
        );
    }

    #[test]
    fn the_crate_root_and_visibility_name_nothing() {
        let found = module_violations(&[
            ("lib.rs", "mod api;\npub use crate::api::router;\n"),
            ("git/zlib.rs", "pub(crate) fn f() {}\n"),
        ]);
        assert!(found.is_empty(), "{found:?}");
    }

    // A moved crate must not turn the rules off without a word.
    #[test]
    fn no_compute_files_is_flagged() {
        let found = check_modules(&[], Path::new("/ws"));
        assert_eq!(found.len(), 1);
        assert!(found[0].message.contains("no Rust files"));
    }

    fn write(path: &Path, contents: &str) {
        fs::create_dir_all(path.parent().expect("has parent")).expect("mkdir");
        fs::write(path, contents).expect("write manifest");
    }

    /// A workspace of `(directory, package name, dependency)` members, laid
    /// out on disk so the rule reads the paths `manifest::load` builds.
    fn workspace(members: &[(&str, &str, &str)]) -> TempDir {
        let dir = tempdir().expect("tempdir");
        let list = members
            .iter()
            .filter(|(path, ..)| *path != ".")
            .map(|(path, ..)| format!("\"{path}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let mut root = format!("[workspace]\nmembers = [{list}]\n");
        for (path, name, dep) in members {
            let manifest = format!("[package]\nname = \"{name}\"\n[dependencies]\n{dep}\n");
            if *path == "." {
                root.push_str(&manifest);
            } else {
                write(&dir.path().join(path).join("Cargo.toml"), &manifest);
            }
        }
        write(&dir.path().join("Cargo.toml"), &root);
        dir
    }

    fn violations(members: &[(&str, &str, &str)]) -> Vec<String> {
        let dir = workspace(members);
        check(&manifest::load(dir.path()).expect("manifests load"))
            .into_iter()
            .map(|violation| violation.message)
            .collect()
    }

    #[test]
    fn an_engine_crate_may_not_depend_on_the_enroute_crate() {
        let found = violations(&[
            ("crates/compute", "enroute-compute", "enroute = \"1\""),
            (".", "enroute", ""),
        ]);
        assert_eq!(found.len(), 1, "expected exactly one upward dependency");
        assert!(found[0].contains("is in the `enroute` layer"), "{found:?}");
    }

    // The whole point of the rule is that it constrains one direction only.
    #[test]
    fn the_enroute_crate_may_depend_on_every_layer() {
        assert!(
            violations(&[
                ("crates/api/types", "enroute-api", ""),
                ("crates/compute", "enroute-compute", ""),
                (
                    ".",
                    "enroute",
                    "enroute-api = \"1\"\nenroute-compute = \"1\"",
                ),
            ])
            .is_empty(),
            "the service is allowed to name every layer"
        );
    }

    /// The `git` layer held only the push client, which is now the compute's.
    #[test]
    fn the_retired_git_layer_must_register_again() {
        let found = violations(&[("crates/git/remote", "enroute-git-remote", "")]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("names no layer"), "{found:?}");
    }

    // A second consumer only tests the contract if it cannot read the engine
    // directly, which is what the `api` rule is for.
    /// The Rust SDK and the example application were both dropped as layers
    /// of their own: there is no SDK, and the application is the customer's.
    ///
    /// Re-adding either as a Rust crate must register a layer deliberately,
    /// not sit in a directory that quietly answers to no rule.
    #[test]
    fn a_reintroduced_sdk_or_application_must_register_its_layer() {
        for dir in ["crates/sdk/git", "crates/hooks/app"] {
            let found = violations(&[(dir, "whatever", "")]);
            assert_eq!(found.len(), 1, "{dir}: {found:?}");
            assert!(found[0].contains("names no layer"), "{dir}: {found:?}");
        }
    }

    // Callers on both ends name the contract, so its dependencies reach both.
    #[test]
    fn the_contract_may_not_depend_on_the_engine() {
        let found = violations(&[
            ("crates/api/types", "enroute-api", "enroute-shared = \"1\""),
            ("crates/shared", "enroute-shared", ""),
        ]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("may name only `api`"), "{found:?}");
    }

    // The compute serves the contract and signs hook calls, so it names `api`.
    #[test]
    fn the_compute_may_depend_on_the_contract() {
        assert!(
            violations(&[
                ("crates/api/types", "enroute-api", ""),
                ("crates/compute", "enroute-compute", "enroute-api = \"1\""),
            ])
            .is_empty(),
            "the compute is allowed to name the contract"
        );
    }

    // Each role runs alone on a node of its own, so neither may need the other.
    #[test]
    fn the_ledger_and_the_compute_may_not_name_each_other() {
        for (from, to) in [("ledger", "compute"), ("compute", "ledger")] {
            let found = violations(&[
                ("crates/shared", "enroute-shared", ""),
                (
                    &format!("crates/{from}"),
                    &format!("enroute-{from}"),
                    &format!("enroute-shared = \"1\"\nenroute-{to} = \"1\""),
                ),
                (&format!("crates/{to}"), &format!("enroute-{to}"), ""),
            ]);
            assert_eq!(found.len(), 1, "{from} -> {to}: {found:?}");
            assert!(
                found[0].contains(&format!("`enroute-{to}` is in the `{to}` layer")),
                "{found:?}"
            );
        }
    }

    // The node runs both roles, so the crate that assembles it names both.
    #[test]
    fn the_enroute_crate_may_name_the_ledger_and_the_compute() {
        assert!(
            violations(&[
                ("crates/ledger", "enroute-ledger", ""),
                ("crates/compute", "enroute-compute", ""),
                (
                    ".",
                    "enroute",
                    "enroute-ledger = \"1\"\nenroute-compute = \"1\"",
                ),
            ])
            .is_empty(),
            "the node is allowed to name both roles"
        );
    }

    /// The service and the node merged into the root package.
    #[test]
    fn the_retired_server_layer_must_register_again() {
        let found = violations(&[("crates/server/app", "enroute-server", "")]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("`crates/server` names no layer"),
            "{found:?}"
        );
    }

    // A layer of one crate is still a layer, so it cannot be one by accident.
    #[test]
    fn a_single_crate_directory_naming_no_layer_is_flagged() {
        let found = violations(&[("crates/proxy", "proxy", "")]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("`crates/proxy` names no layer"),
            "{found:?}"
        );
    }

    // `dev/` tooling drives every layer by design, so it sits outside the
    // rule rather than being an exception inside it.
    #[test]
    fn dev_tooling_answers_to_no_layer() {
        assert!(
            violations(&[("dev/e2e", "e2e", "enroute = \"1\""), (".", "enroute", ""),]).is_empty(),
            "`dev/` is unconstrained"
        );
    }

    // Otherwise a crate could escape the rule by sitting in a directory the
    // rule has never heard of, which is the failure it exists to prevent.
    #[test]
    fn a_crates_directory_naming_no_layer_is_flagged() {
        let found = violations(&[("crates/edge/proxy", "edge-proxy", "")]);
        assert_eq!(found.len(), 1, "expected exactly one unknown-layer report");
        assert!(
            found[0].contains("`crates/edge` names no layer"),
            "{found:?}"
        );
    }
}
