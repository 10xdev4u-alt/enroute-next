# Data model

Enroute stores Git repositories. Your application defines the product model
around those repositories.

## Repositories

Your application creates each repository with a key it chooses, or grants the
key to the push that creates it. Enroute treats the key as opaque and does not
store a display name, owner, description, members, or permissions. Keep those
records in your application and map them to repository keys.

When you enable the `authorize` hook, every Git request requires an answer
from your application. When you do not, every request may read and push, and
the URL path is the key. See
[repository keys](../reference/the-contract.md#repository-keys) and
[Enable hooks](../reference/hooks.md#enable-hooks).

## Objects and refs

A ref is a name that points to an object ID. `UpdateRefs` changes each ref on
its own and does not invoke hooks. It can only target an object already in the
repository, and a branch must point at a commit.

Objects come from Git pushes, or from `CreateCommit`, which makes one commit
from file changes, with at most one parent. Enroute does not create merge
commits; a client must push the merge commit before your application moves a
ref to it. Enroute accepts non-fast-forward updates unless your application
rejects them. Use `IsAncestor` to evaluate ancestry.

Enroute supports SHA-1 repositories. Object IDs are 40 lowercase hexadecimal
characters.

## Namespaces

A deployment serves one application, named in `hooks.endpoint_url`. Repository
keys are unique across the deployment. A key can hold `/`, but Enroute gives
its segments no meaning: it has no concept of an owner or a group.

Grouping repositories is therefore your application's own table, not a field
here. See [Namespacing repositories](../patterns/namespacing.md).

## Exclusions

Enroute does not authenticate users, store product policy, or create Git
objects on its own: every new object comes from a push or a `CreateCommit`
call. See [Hooks](hooks.md) and [Limitations](../reference/limitations.md).
