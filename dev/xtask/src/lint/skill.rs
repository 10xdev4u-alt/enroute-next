//! Holds what the skill ships to what this repository holds.
//!
//! `npx skills add` installs a skill directory and nothing else of the
//! repository, so the skill carries its own copy of the contract. Copies
//! drift, and nothing here builds them, so nothing but this would notice.
//!
//! # What is checked how
//!
//! The contract is byte-identical. The image the skill runs names this
//! release.
//!
//! # The file that names the skill
//!
//! `SKILL.md`'s frontmatter is parsed the way an installer parses it: a block
//! that does not read back is a skill nothing installs at all.

use std::path::{Path, PathBuf};

use walkdir::WalkDir;
use yaml_rust2::{Yaml, YamlLoader};

/// The skill directory, relative to the workspace root.
const SKILL: &str = "skills/build-on-enroute";

/// The file that names the skill to an installer.
const MANIFEST: &str = "SKILL.md";

/// What its frontmatter must name.
///
/// An installer holding neither has nothing to list the skill by, and skips
/// the whole of it.
const REQUIRED: &[&str] = &["name", "description"];

/// The published image, which the skill runs.
const IMAGE: &str = "ghcr.io/enroute-sh/enroute";

/// The pages that tell an agent which image to run, inside the skill.
const RUNS: &[&str] = &[MANIFEST, "references/local-stack.md"];

/// Trees the skill mirrors whole, as `(here, inside the skill)`.
const TREES: &[(&str, &str)] = &[("proto/enroute", "proto/enroute")];

use super::Violation;

pub(crate) fn check(root: &Path) -> Vec<Violation> {
    let mut violations = Vec::new();

    for (here, there) in TREES {
        violations.extend(tree(root, here, there));
    }
    for page in RUNS {
        violations.extend(pin(root, page));
    }
    violations.extend(frontmatter(root));

    violations
}

/// The frontmatter of `SKILL.md`, read by a YAML parser and not by eye.
///
/// An installer parses this before anything else the skill ships, so a block
/// that does not read back is no skill at all.
fn frontmatter(root: &Path) -> Vec<Violation> {
    let file = root.join(SKILL).join(MANIFEST);
    let Ok(text) = std::fs::read_to_string(&file) else {
        return vec![Violation {
            file,
            line: None,
            message: format!("the skill ships no {MANIFEST}, so an installer finds no skill"),
        }];
    };

    findings(&text)
        .into_iter()
        .map(|(line, message)| Violation {
            file: file.clone(),
            line,
            message,
        })
        .collect()
}

/// Everything an installer would refuse this `SKILL.md` for, as `(line, what)`.
fn findings(text: &str) -> Vec<(Option<usize>, String)> {
    let block = match block(text) {
        Ok(block) => block,
        Err(message) => return vec![(Some(1), message.to_string())],
    };

    let documents = match YamlLoader::load_from_str(&block) {
        Ok(documents) => documents,
        // The reason alone: the parser numbers the block it was handed, and
        // this finding is already against the line in the file.
        Err(error) => {
            let line = error.marker().line() + 1;
            return vec![(
                Some(line),
                format!("the frontmatter does not parse: {}", error.info()),
            )];
        }
    };

    let [Yaml::Hash(mapping)] = documents.as_slice() else {
        return vec![(
            None,
            "the frontmatter is not one mapping of keys to values".to_string(),
        )];
    };

    let mut findings = Vec::new();
    for key in REQUIRED {
        match mapping.get(&Yaml::String((*key).to_string())) {
            Some(Yaml::String(value)) if !value.trim().is_empty() => {}
            Some(_) => findings.push((
                line_of(text, key),
                format!("`{key}` is not a string an installer can show"),
            )),
            None => findings.push((None, format!("names no `{key}`, which an installer needs"))),
        }
    }
    findings
}

/// What sits between the opening `---` and the one that closes it.
fn block(text: &str) -> Result<String, &'static str> {
    let Some(rest) = text.strip_prefix("---\n") else {
        return Err("opens with no `---`, so an installer reads no frontmatter");
    };

    let mut block = String::new();
    for line in rest.lines() {
        if line.trim_end() == "---" {
            return Ok(block);
        }
        block.push_str(line);
        block.push('\n');
    }
    Err("opens frontmatter that no closing `---` ends")
}

/// The line a key sits on, for a finding that points at it.
fn line_of(text: &str, key: &str) -> Option<usize> {
    text.lines()
        .position(|line| line.starts_with(&format!("{key}:")))
        .map(|index| index + 1)
}

/// The image the skill tells an agent to run, which must be this release.
///
/// A skill ships beside one release and its proto describes that Enroute, so
/// pinning is what keeps an old skill correct instead of silently newer.
fn pin(root: &Path, page: &str) -> Vec<Violation> {
    let file = root.join(SKILL).join(page);
    let (Some(version), Ok(text)) = (workspace_version(root), std::fs::read_to_string(&file))
    else {
        return Vec::new();
    };

    let pins = pinned(&text);
    if pins.is_empty() {
        return vec![Violation {
            file,
            line: None,
            message: format!("names no Enroute image: expected `{IMAGE}:{version}`"),
        }];
    }
    pins.into_iter()
        .filter(|(_, pinned)| *pinned != version)
        .map(|(line, pinned)| Violation {
            file: file.clone(),
            line: Some(line),
            message: format!("starts Enroute {pinned}; this release is {version}"),
        })
        .collect()
}

/// `version` from `[workspace.package]`.
fn workspace_version(root: &Path) -> Option<String> {
    let text = std::fs::read_to_string(root.join("Cargo.toml")).ok()?;
    // `toml::from_str` and not `str::parse`, which reads one TOML value rather
    // than a document and so fails on every manifest.
    let manifest: toml::Value = toml::from_str(&text).ok()?;
    let version = manifest
        .get("workspace")?
        .get("package")?
        .get("version")?
        .as_str()?;
    Some(version.to_string())
}

/// Each tag the text names the image by, as `(line, tag)`.
///
/// Whatever follows the image name, so a moving tag and a variable are both
/// read rather than skipped: each is a way for the pin to stop being one.
fn pinned(text: &str) -> Vec<(usize, String)> {
    let prefix = format!("{IMAGE}:");
    let mut pins = Vec::new();
    for (index, line) in text.lines().enumerate() {
        for rest in line.split(prefix.as_str()).skip(1) {
            let tag: String = rest
                .chars()
                .take_while(|c| !c.is_whitespace() && *c != '`')
                .collect();
            pins.push((index + 1, tag));
        }
    }
    pins
}

/// Every `.proto` under both, matched in both directions.
fn tree(root: &Path, here: &str, there: &str) -> Vec<Violation> {
    let source = root.join(here);
    let copy = root.join(SKILL).join(there);
    if !source.exists() {
        return Vec::new();
    }
    // Gone wholesale is the loudest way this drifts, and comparing nothing to
    // nothing is how it would go unsaid.
    if !copy.exists() {
        return vec![Violation {
            file: copy,
            line: None,
            message: format!("the skill ships no copy of {here} at all"),
        }];
    }

    let mut violations = Vec::new();
    for relative in protos(&source) {
        let theirs = copy.join(&relative);
        let Ok(ours) = std::fs::read(source.join(&relative)) else {
            continue;
        };
        match std::fs::read(&theirs) {
            Err(_) => violations.push(Violation {
                file: theirs,
                line: None,
                message: format!(
                    "the skill ships no copy of {here}/{}: cp -R {here} {SKILL}/{there}/..",
                    relative.display()
                ),
            }),
            Ok(found) if found != ours => violations.push(stale(&theirs, here, &relative)),
            Ok(_) => {}
        }
    }

    // The other direction, so dropping a file from the contract does not leave
    // the skill shipping one the server has never heard of.
    for relative in protos(&copy) {
        if !source.join(&relative).exists() {
            violations.push(Violation {
                file: copy.join(&relative),
                line: None,
                message: format!("{here}/{} is gone; delete this copy", relative.display()),
            });
        }
    }

    violations
}

/// Every `.proto` under `dir`, named relative to it.
fn protos(dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(dir)
        .into_iter()
        .filter_map(Result::ok)
        .map(walkdir::DirEntry::into_path)
        .filter(|path| path.extension().is_some_and(|ext| ext == "proto"))
        .filter_map(|path| path.strip_prefix(dir).ok().map(Path::to_path_buf))
        .collect()
}

fn stale(file: &Path, here: &str, relative: &Path) -> Violation {
    Violation {
        file: file.to_path_buf(),
        line: None,
        message: format!(
            "differs from {here}/{}: an agent installing the skill would generate the wrong contract",
            relative.display()
        ),
    }
}

#[cfg(test)]
mod frontmatter_tests {
    use super::findings;

    /// A `SKILL.md` whose frontmatter holds `entries`.
    fn skill(entries: &str) -> String {
        format!("---\n{entries}---\n\n# Build on Enroute\n\nProse.\n")
    }

    #[test]
    fn a_frontmatter_that_parses_and_names_both_keys_is_a_finding_of_nothing() {
        let text = skill("name: build-on-enroute\ndescription: Start a stack.\n");
        assert_eq!(findings(&text), Vec::new());
    }

    /// The bug this rule exists for: a plain scalar ends at `: `, so the rest
    /// of the description reads as a mapping nested where none may sit.
    #[test]
    fn a_colon_in_a_plain_scalar_is_the_nested_mapping_it_reads_as() {
        let text = skill("name: build-on-enroute\ndescription: patterns: branches, CI.\n");
        let findings = findings(&text);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].0, Some(3));
        assert!(findings[0].1.starts_with("the frontmatter does not parse"));
    }

    /// The same description, quoted, which is what the colon costs.
    #[test]
    fn a_quoted_colon_is_one_value() {
        let text = skill("name: build-on-enroute\ndescription: \"patterns: branches, CI.\"\n");
        assert_eq!(findings(&text), Vec::new());
    }

    #[test]
    fn an_unclosed_quote_is_caught_the_same_way() {
        let text = skill("name: build-on-enroute\ndescription: \"branches, CI.\n");
        let findings = findings(&text);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].1.starts_with("the frontmatter does not parse"));
    }

    #[test]
    fn a_file_that_opens_no_frontmatter_names_nothing() {
        let findings = findings("# Build on Enroute\n\nProse.\n");
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].1.contains("opens with no `---`"));
    }

    /// An unclosed block swallows the prose below it, which parses or does not
    /// depending on what the prose holds. Either way it is not frontmatter.
    #[test]
    fn a_frontmatter_no_second_marker_closes_is_not_one() {
        let findings = findings("---\nname: build-on-enroute\n\n# Build on Enroute\n");
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].1.contains("no closing `---`"));
    }

    #[test]
    fn a_key_an_installer_needs_is_reported_by_name() {
        let findings = findings(&skill("name: build-on-enroute\n"));
        assert_eq!(
            findings,
            vec![(
                None,
                "names no `description`, which an installer needs".to_string()
            )]
        );
    }

    /// Present but empty is the same to whoever lists the skill, and a parser
    /// has no complaint about it.
    #[test]
    fn a_key_that_holds_nothing_is_reported_against_its_line() {
        let text = skill("name: build-on-enroute\ndescription: \"\"\n");
        let findings = findings(&text);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].0, Some(3));
    }

    #[test]
    fn a_key_that_holds_a_list_is_no_description() {
        let text = skill("name: build-on-enroute\ndescription: [a, b]\n");
        let findings = findings(&text);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].1.contains("not a string"));
    }
}

#[cfg(test)]
mod tests {
    use super::pinned;

    fn tags(text: &str) -> Vec<String> {
        pinned(text).into_iter().map(|(_, tag)| tag).collect()
    }

    #[test]
    fn it_reads_the_version_a_command_runs() {
        let text = "docker run --rm \\\n  ghcr.io/enroute-sh/enroute:0.1.0-alpha.1 dev\n";
        assert_eq!(pinned(text), vec![(2, "0.1.0-alpha.1".to_string())]);
    }

    #[test]
    fn it_reads_every_tag_and_stops_at_a_backtick() {
        let text = "`ghcr.io/enroute-sh/enroute:0.3.0`, then ghcr.io/enroute-sh/enroute:0.2.0\n";
        assert_eq!(tags(text), ["0.3.0", "0.2.0"]);
    }

    /// A moving tag is the drift this rule exists to catch, so it has to be
    /// read rather than skipped.
    #[test]
    fn a_channel_tag_reads_as_the_pin_it_is_not() {
        assert_eq!(tags("ghcr.io/enroute-sh/enroute:latest dev\n"), ["latest"]);
    }

    /// So does a variable: it is a pin that whoever runs the command decides.
    #[test]
    fn a_variable_reads_as_the_pin_it_is_not() {
        let text = "ghcr.io/enroute-sh/enroute:${ENROUTE_VERSION:-alpha} dev\n";
        assert_eq!(tags(text), ["${ENROUTE_VERSION:-alpha}"]);
    }

    #[test]
    fn the_bare_image_name_is_no_pin() {
        assert!(pinned("Every release is `ghcr.io/enroute-sh/enroute`.\n").is_empty());
    }
}
