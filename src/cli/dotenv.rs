//! `ENROUTE_*` values from `.env` and `.env.local` in the working directory.
//!
//! They become argument defaults rather than environment variables: setting
//! the environment is `unsafe` in this edition. So the order is a flag, then
//! the real environment, then `.env.local`, then `.env`.

use std::collections::BTreeMap;
use std::path::Path;

/// `command` with each `ENROUTE_*` argument defaulted from the files in `dir`.
pub(crate) fn with_defaults(command: clap::Command, dir: &Path) -> clap::Command {
    let mut values = BTreeMap::new();
    for name in [".env", ".env.local"] {
        values.extend(read(&dir.join(name)));
    }
    values.retain(|key, _| std::env::var_os(key).is_none());
    if values.is_empty() {
        return command;
    }
    defaulted(command, &values)
}

fn defaulted(mut command: clap::Command, values: &BTreeMap<String, String>) -> clap::Command {
    let ids: Vec<(clap::Id, String)> = command
        .get_arguments()
        .filter_map(|arg| {
            let env = arg.get_env()?.to_str()?;
            Some((arg.get_id().clone(), values.get(env)?.clone()))
        })
        .collect();
    for (id, value) in ids {
        // Leaked: clap keeps a default for the life of the process anyway.
        let value: &'static str = Box::leak(value.into_boxed_str());
        command = command.mut_arg(id, |arg| match arg.get_value_delimiter() {
            Some(delimiter) => arg.default_values(value.split(delimiter)),
            None => arg.default_value(value),
        });
    }
    let names: Vec<String> = command
        .get_subcommands()
        .map(|sub| sub.get_name().to_string())
        .collect();
    for name in names {
        command = command.mut_subcommand(name, |sub| defaulted(sub, values));
    }
    command
}

/// The `ENROUTE_*` assignments in the dotenv file at `path`, if it exists.
///
/// A line dotenvy cannot read is skipped, as one with another key is.
fn read(path: &Path) -> Vec<(String, String)> {
    let Ok(lines) = dotenvy::from_path_iter(path) else {
        return Vec::new();
    };
    lines
        .filter_map(Result::ok)
        .filter(|(key, _)| key.starts_with("ENROUTE_"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_enroute_keys_are_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".env");
        std::fs::write(
            &path,
            "# a comment\nENROUTE_API=http://x:1 # inline\nexport ENROUTE_DEV_DIR=\"/tmp/d\"\n\
             OTHER=1\nnot a line\nENROUTE_HOOKS_URL='http://h'\n",
        )
        .unwrap();
        let parsed = read(&path);
        assert_eq!(
            parsed,
            [
                ("ENROUTE_API".to_string(), "http://x:1".to_string()),
                ("ENROUTE_DEV_DIR".to_string(), "/tmp/d".to_string()),
                ("ENROUTE_HOOKS_URL".to_string(), "http://h".to_string()),
            ]
        );
    }

    #[test]
    fn a_file_value_becomes_a_default_and_a_flag_wins() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(".env"), "ENROUTE_TEST_ONLY_X=from-env\n").unwrap();
        std::fs::write(dir.path().join(".env.local"), "ENROUTE_TEST_ONLY_X=local\n").unwrap();
        let command = || {
            clap::Command::new("t").subcommand(
                clap::Command::new("s")
                    .arg(clap::Arg::new("x").long("x").env("ENROUTE_TEST_ONLY_X")),
            )
        };
        let read = |args: &[&str]| {
            let matches = with_defaults(command(), dir.path()).get_matches_from(args);
            let (_, sub) = matches.subcommand().unwrap();
            sub.get_one::<String>("x").cloned()
        };
        assert_eq!(read(&["t", "s"]).as_deref(), Some("local"));
        assert_eq!(read(&["t", "s", "--x", "flag"]).as_deref(), Some("flag"));
    }
}
