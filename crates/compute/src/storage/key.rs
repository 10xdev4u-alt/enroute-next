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
            .join(self.repo.replace('/', "%2F"))
            .join(format!("{}.layer", self.id))
    }
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

    /// The escaping is injective only over repositories that exist, which is
    /// what keeps two of them from sharing a cache directory. The invariant it
    /// rests on lives in the key rules and not here, so the test names both
    /// halves: the escaped name is what this transform produces, and no key
    /// could be that name.
    #[test]
    fn escaping_a_key_produces_a_name_no_key_could_be() {
        use crate::contract::repo_key::RepoKey;

        let root = Path::new("/cache");
        let dir = |repo: &str| {
            FileKey::new(repo, "id")
                .local_path(root)
                .parent()
                .map(Path::to_path_buf)
                .unwrap()
        };
        let escaped = dir("acme/widgets");
        assert_eq!(escaped, dir("acme%2Fwidgets"));
        assert_eq!(escaped.file_name().unwrap(), "acme%2Fwidgets");
        // The one line that makes the collision above unreachable.
        assert!(!RepoKey::is_valid("acme%2Fwidgets"));
        assert!(RepoKey::is_valid("acme/widgets"));
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
}
