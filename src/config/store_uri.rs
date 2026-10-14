//! Where a store's bytes live, as one string.
//!
//! A repository's objects are named this way. One thing more is named the same
//! way and is not a store: [`ObjectUri`] is a single object inside one, which
//! is how the deployment file is read.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow, bail};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreScheme, memory::InMemory, prefix::PrefixStore};
use serde::Deserialize;
use url::Url;

use crate::config::Secret;
use crate::node::Store;

/// Where a store's bytes live, as a URL: `s3://bucket/prefix`,
/// `gs://`, `az://`, `file://`, `memory://`, or `https://`.
///
/// Whatever [`object_store`] can reach, since the backend is a deployment's
/// choice and not one this repository should be making for it.
#[derive(Clone, Hash)]
pub struct StoreUri {
    /// The location, with the query stripped: those are ours, not the URL's.
    url: Url,
    /// The query, as the backend's own configuration keys.
    options: Vec<(String, String)>,
}

impl std::fmt::Debug for StoreUri {
    /// The location and its options, both in full.
    ///
    /// Safe because `from_str`, the only writer of `options`, refuses a
    /// credential-shaped name, and `build_with` stores none it folds in.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreUri")
            .field("url", &self.url.as_str())
            .field("options", &self.options)
            .finish()
    }
}

/// Fold the URI's options into a builder, over its environment.
///
/// `from_env` first, so credentials keep arriving the way each SDK expects,
/// and an unparseable key is an error, not a quiet drop.
macro_rules! configured {
    ($builder:ty, $uri:expr, $options:expr) => {{
        let mut builder = <$builder>::from_env().with_url($uri.url.as_str());
        for (key, value) in $options {
            builder = builder.with_config($uri.key(key)?, value);
        }
        // Bound with its type rather than cast: a cast here is `trivial_casts`.
        let store: Arc<dyn ObjectStore> = Arc::new(
            builder
                .build()
                .with_context(|| format!("building a store for {}", $uri.url))?,
        );
        store
    }};
}

/// A store at the path inside it, which is where a caller wants it.
fn rooted((store, path): (Arc<dyn ObjectStore>, Path)) -> Arc<dyn ObjectStore> {
    if path.as_ref().is_empty() {
        store
    } else {
        Arc::new(PrefixStore::new(store, path))
    }
}

impl StoreUri {
    /// The store this names, rooted at the path inside it, with `extra`
    /// folded in after the URI's own.
    ///
    /// # Errors
    ///
    /// Returns an error if the scheme is one this build cannot reach, if an
    /// option is not one that backend takes, or if the store cannot be built.
    fn build(&self, extra: &[(String, String)]) -> Result<Arc<dyn ObjectStore>> {
        Ok(rooted(self.store(extra)?))
    }

    /// The store this URL's scheme names, and the path inside it — which
    /// [`build`] roots the store at and [`ObjectUri`] reads as an object.
    ///
    /// [`build`]: Self::build
    fn store(&self, extra: &[(String, String)]) -> Result<(Arc<dyn ObjectStore>, Path)> {
        let (scheme, path) = ObjectStoreScheme::parse(&self.url)
            .with_context(|| format!("{} is not a store URL", self.url))?;
        // `extra` last, so a credential supplied at runtime beats whatever the
        // URI said — which is the only reason a caller passes one.
        let options = || self.options.iter().chain(extra);

        let store: Arc<dyn ObjectStore> = match scheme {
            ObjectStoreScheme::AmazonS3 => {
                configured!(object_store::aws::AmazonS3Builder, self, options())
            }
            ObjectStoreScheme::GoogleCloudStorage => {
                configured!(
                    object_store::gcp::GoogleCloudStorageBuilder,
                    self,
                    options()
                )
            }
            ObjectStoreScheme::MicrosoftAzure => {
                configured!(object_store::azure::MicrosoftAzureBuilder, self, options())
            }
            // No `from_env`: this one has no environment of its own.
            ObjectStoreScheme::Http => {
                let mut builder =
                    object_store::http::HttpBuilder::new().with_url(self.url.as_str());
                for (key, value) in options() {
                    builder = builder.with_config(self.key(key)?, value);
                }
                Arc::new(builder.build().context("building an HTTP store")?)
            }
            ObjectStoreScheme::Local => {
                self.takes_no_options(extra)?;
                // Rooted at `/`, with the path applied below like any other
                // scheme's — `new_with_prefix` would demand it already exist.
                Arc::new(object_store::local::LocalFileSystem::new())
            }
            ObjectStoreScheme::Memory => {
                self.takes_no_options(extra)?;
                Arc::new(InMemory::new())
            }
            other => return Err(anyhow!("{other:?} is not a store this build can reach")),
        };

        Ok((store, path))
    }

    /// One of this backend's configuration keys, by name.
    ///
    /// The source is dropped deliberately: it says only that the key is
    /// unknown, which is what this says already and with the scheme attached.
    fn key<K: std::str::FromStr>(&self, key: &str) -> Result<K> {
        key.to_ascii_lowercase().parse().map_err(|_unknown_key| {
            anyhow!("{key} is not an option a {} store takes", self.url.scheme())
        })
    }

    /// # Errors
    ///
    /// Returns an error if any option was given, for a backend with none.
    fn takes_no_options(&self, extra: &[(String, String)]) -> Result<()> {
        match self.options.first().or_else(|| extra.first()) {
            None => Ok(()),
            Some((key, _)) => Err(anyhow!(
                "{key} is not an option a {} store takes",
                self.url.scheme()
            )),
        }
    }
}

/// A store, and whatever it takes to reach one.
///
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bucket {
    /// Where the bytes live.
    #[serde(deserialize_with = "crate::config::expand::parsed::deserialize")]
    pub uri: StoreUri,
    /// The backend's own credential keys, by their `object_store` names.
    ///
    /// Empty for a deployment whose identity is a role the SDK finds for
    /// itself, which is the arrangement to prefer where there is one.
    #[serde(default)]
    pub credentials: BTreeMap<String, Secret>,
}

impl Bucket {
    /// The store this names, reached with the credentials beside it.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be built.
    pub fn store(&self) -> Result<Store> {
        if self.uri.url.scheme() == "file" {
            self.uri.takes_no_options(&[])?;
            let root = self
                .uri
                .url
                .to_file_path()
                .ok()
                .with_context(|| format!("{} is not a local path", self.uri.url))?;
            return Store::local(&root);
        }
        Ok(Store::new(self.uri.build(&self.pairs())?))
    }

    /// The option pairs a builder folds in.
    fn pairs(&self) -> Vec<(String, String)> {
        self.credentials
            .iter()
            .map(|(name, value)| (name.clone(), value.expose().to_string()))
            .collect()
    }
}

/// One object inside a store, named the way a store is: the backend's URL
/// with the object's own path on the end.
///
/// `s3://config/enroute.toml` and `file:///etc/enroute/enroute.toml` are the
/// same string shape, so which backend holds them is not a build.
#[derive(Clone, Debug)]
pub struct ObjectUri(StoreUri);

impl ObjectUri {
    /// The store holding it, and where it sits inside that store.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be built.
    pub fn build(&self) -> Result<(Arc<dyn ObjectStore>, Path)> {
        self.0.store(&[])
    }

    /// The URL this was read from, for saying where something was read.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.url.as_str()
    }
}

impl std::fmt::Display for ObjectUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ObjectUri {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let uri: StoreUri = s.parse()?;
        // A URL with only a bucket names a store and nothing in it. Refused
        // here so it is a usage error rather than a 404 on the first read.
        let (_, path) = ObjectStoreScheme::parse(&uri.url)?;
        if path.as_ref().is_empty() {
            bail!("{s} names a store and no object in it");
        }
        Ok(Self(uri))
    }
}

/// Deserialize a URI as the string it parses from.
///
/// No `${VAR}` here: the file's fields expand before they parse.
///
/// [`FromStr`]: std::str::FromStr
macro_rules! deserialize_from_str {
    ($uri:ty) => {
        impl<'de> serde::Deserialize<'de> for $uri {
            fn deserialize<D: serde::Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<Self, D::Error> {
                // `{:#}` for the whole chain: the outer message alone says a
                // string is not a URL without saying which part of it is not.
                <String as serde::Deserialize>::deserialize(deserializer)?
                    .parse()
                    .map_err(|error| serde::de::Error::custom(format!("{error:#}")))
            }
        }
    };
}

deserialize_from_str!(StoreUri);
deserialize_from_str!(ObjectUri);

impl std::str::FromStr for StoreUri {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut url = Url::parse(s).with_context(|| format!("{s} is not a URL"))?;
        let options: Vec<(String, String)> = url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        // The backend never sees them as a query: they are how it is built.
        url.set_query(None);

        // Rejected at parse, so a bad URL is a usage error rather than
        // something that surfaces on the first push.
        ObjectStoreScheme::parse(&url).with_context(|| format!("{s} is not a store URL"))?;

        no_credentials(&options)?;
        Ok(Self { url, options })
    }
}

/// Refuse a credential written into a URI's query.
///
/// A query value is form-decoded, so a `+` in it becomes a space — and a
/// base64 secret key holds `+` often. It would fail as a 403 naming nothing.
///
/// # Errors
///
/// Returns an error if an option name looks like a credential's.
fn no_credentials(options: &[(String, String)]) -> Result<()> {
    const LOOKS_SECRET: &[&str] = &["key", "secret", "token", "password", "credential"];

    for (name, _) in options {
        let lowered = name.to_ascii_lowercase();
        if LOOKS_SECRET.iter().any(|part| lowered.contains(part)) {
            bail!(
                "{name} is a credential, so it belongs in this store's \
                 `credentials` rather than its URI: a query value is \
                 form-decoded, and a `+` in one becomes a space"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uri(s: &str) -> StoreUri {
        s.parse().unwrap()
    }

    /// What a store was configured with is what an operator reads a log for.
    #[test]
    fn the_options_a_store_was_built_with_are_printed() {
        let printed = format!(
            "{:?}",
            uri("s3://objects?endpoint=https://s3.example.com&s3_express=true")
        );

        assert!(printed.contains("s3.example.com"), "{printed}");
        assert!(printed.contains("s3_express"), "{printed}");
    }

    /// Why that is safe, as a test rather than as two lists agreeing: the only
    /// writer of `options` refuses what `Debug` would otherwise expose.
    #[test]
    fn a_credential_can_never_reach_that_debug() {
        for raw in [
            "s3://objects?secret_access_key=hunter2",
            "s3://objects?access_key_id=hunter2",
            "az://objects?sas_key=hunter2",
        ] {
            raw.parse::<StoreUri>()
                .expect_err("refused by the constructor, so Debug never sees it");
        }
    }

    /// A credential does not belong in a query at all, because a query value
    /// is form-decoded and a base64 key holds `+` often.
    #[test]
    fn a_credential_in_a_uri_is_refused_where_it_is_written() {
        for raw in [
            "s3://objects?secret_access_key=abc",
            "s3://objects?access_key_id=abc",
            "az://objects?sas_key=abc",
            "gs://objects?service_account_key=abc",
            "s3://objects?session_token=abc",
        ] {
            let error = raw.parse::<StoreUri>().expect_err(raw).to_string();
            assert!(error.contains("credentials"), "{error}");
        }
    }

    /// The reason for that refusal, pinned: `+` in a query value is a space,
    /// and a secret key that lost one fails as a 403 naming nothing.
    #[test]
    fn a_query_value_is_form_decoded() {
        let uri = uri("s3://objects?endpoint=ab+cd");
        assert_eq!(uri.options, [("endpoint".to_string(), "ab cd".to_string())]);
    }

    #[test]
    fn every_backend_this_build_carries_is_reachable() {
        for s in [
            "s3://objects/prefix",
            "gs://objects/prefix",
            "az://objects/prefix?account_name=someone",
            "file:///tmp/objects",
            "memory:///",
            "https://example.com/objects",
        ] {
            uri(s).build(&[]).unwrap_or_else(|e| panic!("{s}: {e:#}"));
        }
    }

    #[test]
    fn options_are_the_backend_s_own_config_keys() {
        // A directory bucket is a property of the bucket, so it is said here
        // rather than by whoever happens to be building the store. The zone
        // suffix is what object_store then holds the name to, which is the
        // proof the option reached it.
        uri("s3://scratch--use1-az4--x-s3?s3_express=true&region=us-east-1")
            .build(&[])
            .unwrap();
        let error = format!(
            "{:#}",
            uri("s3://scratch?s3_express=true").build(&[]).unwrap_err()
        );
        assert!(error.contains("Zone suffix"), "{error}");
    }

    #[test]
    fn an_unknown_option_is_refused() {
        let error = format!("{:#}", uri("s3://objects?nope=1").build(&[]).unwrap_err());
        assert!(error.contains("nope"), "{error}");

        let error = format!("{:#}", uri("file:///tmp/x?nope=1").build(&[]).unwrap_err());
        assert!(error.contains("nope"), "{error}");
    }

    /// The store and the key inside it, not a store rooted at the key — an
    /// object is read with `get`, and `build` would have made it a prefix.
    #[test]
    fn an_object_uri_keeps_the_path_as_the_object() {
        for (s, key) in [
            ("s3://config/enroute.toml", "enroute.toml"),
            ("s3://config/a/b/enroute.toml", "a/b/enroute.toml"),
            (
                "file:///etc/enroute/enroute.toml",
                "etc/enroute/enroute.toml",
            ),
            ("memory:///enroute.toml", "enroute.toml"),
        ] {
            let (_store, path) = s
                .parse::<ObjectUri>()
                .unwrap_or_else(|e| panic!("{s}: {e:#}"))
                .build()
                .unwrap_or_else(|e| panic!("{s}: {e:#}"));
            assert_eq!(path.as_ref(), key, "{s}");
        }
    }

    /// A bucket with no key names nowhere to read from, which is a usage
    /// error rather than a 404 on the first refresh.
    #[test]
    fn an_object_uri_naming_no_object_is_refused() {
        for s in ["s3://config", "file:///", "memory:///"] {
            let error = format!("{:#}", s.parse::<ObjectUri>().unwrap_err());
            assert!(error.contains("no object in it"), "{s}: {error}");
        }
    }

    #[test]
    fn a_scheme_with_no_backend_is_refused_at_parse() {
        "wat://objects".parse::<StoreUri>().unwrap_err();
        "not a url".parse::<StoreUri>().unwrap_err();
    }
}
