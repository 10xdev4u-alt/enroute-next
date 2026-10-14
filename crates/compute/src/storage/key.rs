//! Names of layer files, independent of where their bytes are (memory,
//! local disk, the object store).

use std::path::{Path, PathBuf};

use object_store::path::Path as StorePath;

/// A layer file: a repository and the layer's id (a random name, never reused).
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(super) struct FileKey {
    pub repo: String,
    pub id: String,
}

impl FileKey {
    pub(crate) fn new(repo: &str, id: &str) -> FileKey {
        FileKey {
            repo: repo.to_string(),
            id: id.to_string(),
        }
    }

    /// Key in the object store.
    pub(crate) fn store_path(&self) -> StorePath {
        StorePath::from(format!("repos/{}/layers/{}.layer", self.repo, self.id))
    }

    /// Prefix of `repo`'s layers in the object store.
    pub(crate) fn store_prefix(repo: &str) -> StorePath {
        StorePath::from(format!("repos/{repo}/layers"))
    }

    /// Path of the local copy under `root`.
    pub(crate) fn local_path(&self, root: &Path) -> PathBuf {
        root.join("layers")
            .join(local_dir_name(&self.repo))
            .join(format!("{}.layer", self.id))
    }
}

/// The name of the local directory holding one repository's layers.
///
/// A key's `/` separates directories in the object store and must not here:
/// the cache keeps one directory per repository. The escape stays injective
/// because no key contains `%`. That is a rule of `contract::repo_key`, which
/// asserts the coupling from its side, so this module keeps the two
/// dependencies the layer rule allows it.
pub(crate) fn local_dir_name(repo: &str) -> String {
    repo.replace('/', "%2F")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_store_path_is_a_layer_of_its_repository() {
        let key = FileKey::new("acme/widgets", "0123abcd");
        assert_eq!(
            key.store_path().as_ref(),
            "repos/acme/widgets/layers/0123abcd.layer"
        );
        // The sweep lists this prefix and must find that path under it.
        let prefix = FileKey::store_prefix("acme/widgets");
        assert_eq!(prefix.as_ref(), "repos/acme/widgets/layers");
        assert!(key.store_path().as_ref().starts_with(prefix.as_ref()));
    }

    /// The store keeps a key's `/` as a separator. The local cache must not:
    /// one directory per repository, so a namespaced key stays one level.
    #[test]
    fn a_namespaced_key_is_one_local_directory() {
        let key = FileKey::new("acme/widgets", "0123abcd");
        assert_eq!(
            key.local_path(Path::new("/cache")),
            Path::new("/cache/layers/acme%2Fwidgets/0123abcd.layer")
        );
    }

    #[test]
    fn distinct_keys_get_distinct_local_directories() {
        let root = Path::new("/cache");
        let dir = |repo: &str| {
            FileKey::new(repo, "id")
                .local_path(root)
                .parent()
                .map(Path::to_path_buf)
                .unwrap()
        };
        let dirs: Vec<_> = [
            "acme",
            "acme/widgets",
            "acme-widgets",
            "acme_widgets",
            "acme.widgets",
            "a/b/c",
            "a/b",
        ]
        .iter()
        .map(|repo| dir(repo))
        .collect();
        for (i, a) in dirs.iter().enumerate() {
            for (j, b) in dirs.iter().enumerate() {
                assert_eq!(i == j, a == b, "{a:?} vs {b:?}");
            }
        }
    }

    /// The field names are part of the stored form: a key is written into the
    /// manifest, so a rename would make an existing manifest unreadable.
    #[test]
    fn a_key_round_trips_through_serde() {
        let key = FileKey::new("acme/widgets", "0123abcd");
        let encoded = serde_json::to_string(&key).unwrap();
        assert_eq!(encoded, r#"{"repo":"acme/widgets","id":"0123abcd"}"#);
        assert_eq!(serde_json::from_str::<FileKey>(&encoded).unwrap(), key);
    }
}
