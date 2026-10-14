# Quickstart

This guide runs Enroute on your machine, pushes a repository to it, and reads
the repository back. You need Git and the `enroute` binary.

## Install

On Linux or macOS, run the installer:

```sh
curl -fsSL https://enroute.sh/install | sh
```

It downloads the latest release for your platform, checks it against
`SHA256SUMS`, and puts `enroute` in `~/.local/bin`. To install a different
release, set `ENROUTE_VERSION`, for example `0.3.0`. To install to a
different directory, set `ENROUTE_INSTALL_DIR`. Pass the variables to `sh`:

```sh
curl -fsSL https://enroute.sh/install | ENROUTE_VERSION=0.3.0 sh
```

You can also download the archive for your platform from the
[releases page](https://github.com/enroute-sh/enroute/releases), and put
`enroute` on your `PATH`:

| Platform | Archive |
| --- | --- |
| Linux, x86-64 | `enroute-<version>-x86_64-unknown-linux-gnu.tar.gz` |
| Linux, ARM64 | `enroute-<version>-aarch64-unknown-linux-gnu.tar.gz` |
| macOS, Apple silicon | `enroute-<version>-aarch64-apple-darwin.tar.gz` |

```sh
tar -xzf enroute-<version>-<target>.tar.gz
sudo install enroute-<version>-<target>/enroute /usr/local/bin/
```

`SHA256SUMS` on the same page has the checksum of each archive. The Linux
binaries need glibc 2.36 or later. The macOS binary is not signed. If you
download it with a browser, macOS blocks it. To let it run, enter
`xattr -d com.apple.quarantine /usr/local/bin/enroute`.

For a different platform, build `enroute` from a checkout of the repository.
You need Rust:

```sh
cargo install --path .
```

You can also [run the published image](#run-the-image).

## Start Enroute

```sh
enroute dev
```

`enroute dev` runs one node with a generated configuration. It keeps its data
in `./.enroute/dev`, and writes a `.gitignore` in `./.enroute` so that Git
ignores it.

```text

enroute dev is running. Press Ctrl-C to stop.

  git      http://127.0.0.1:8080
  api      http://127.0.0.1:50051
  dir      /home/you/project/.enroute/dev
  hooks    off (every request may read and push; pass --hooks URL to opt in)

Try:
  git push http://127.0.0.1:8080/hello.git main
  enroute repo list
```

All listeners bind to `127.0.0.1`. To bind Git and the API to a different
address, use `--host`. No hook is enabled, so every request may read and push.

## Push a repository

In another terminal, make a repository and push it:

```sh
git init -b main hello && cd hello
echo "# hello" > README.md
git add README.md && git commit -m "First commit"
git push http://127.0.0.1:8080/hello.git main
```

The push creates the repository. Its key is the URL path without `.git`:
`hello`. See [Repository keys](reference/the-contract.md#repository-keys).

List the repositories:

```sh
enroute repo list
```

## Read it back

Clone the repository:

```sh
git clone http://127.0.0.1:8080/hello.git hello-copy
```

Make a commit without a checkout. This calls `CreateCommit` on the API, as
your application does:

```sh
enroute commit hello --branch main -m "Add notes" \
  --author "You <you@example.com>" --put notes.md="hello"
```

Run `git pull` in `hello-copy` to get the commit.

The `repo`, `refs`, and `commit` commands call the gRPC API. To call it
directly, use a gRPC client such as [`grpcurl`]. The server answers
reflection.

[`grpcurl`]: https://github.com/fullstorydev/grpcurl

## Opt in to hooks

Hooks let your application decide who may read and push. Every hook is
opt-in. Stop `enroute dev` with Ctrl-C, and start it with a hook URL:

```sh
enroute dev --hooks http://localhost:3000/api/enroute/hooks --enable authorize
```

On the first run, Enroute generates a signing key and keeps it in
`.enroute/dev`. The banner prints the public key, its key ID, and the path of
`hooks-signing-key.pub.pem`. Your application verifies each hook with this
public key. See [Verify the signature](reference/verify-the-signature.md).

With `authorize` enabled, Enroute asks your application about every Git
request. Your application maps the URL to a repository key, identifies the
caller, and grants or denies access. Until an endpoint answers at that URL,
every Git request fails. Leave out `--enable` to enable all four hooks.

To build the endpoint, follow
[Build a code-hosting application](build/README.md). For every field, see the
[hook reference](reference/hooks.md).

## Run the image

You can run `enroute dev` from the published image instead of a local build.
Replace `<version>` with a release, for example `0.3.0`:

```sh
docker run --rm -p 8080:8080 -p 50051:50051 -v enroute-dev:/data \
  ghcr.io/enroute-sh/enroute:<version> dev --dir /data --host 0.0.0.0
```

`--host 0.0.0.0` binds Git and the API to all interfaces in the container,
because the host cannot reach the loopback address of a container. With no
`authorize` hook, Enroute logs a warning that anyone who can reach Git can
read and push. Publish the ports only to a machine that you trust. The
`enroute-dev` volume keeps the data. To delete the data, run
`docker volume rm enroute-dev`.

To send hooks to an application on the host, add a hook URL that names
`host.docker.internal`:

```sh
docker run --rm -p 8080:8080 -p 50051:50051 -v enroute-dev:/data \
  --add-host=host.docker.internal:host-gateway \
  ghcr.io/enroute-sh/enroute:<version> dev --dir /data --host 0.0.0.0 \
  --hooks http://host.docker.internal:3000/api/enroute/hooks --enable authorize
```

`--add-host` is necessary on Linux. Docker Desktop resolves
`host.docker.internal` without it.

The public signing key is in the volume, at
`/data/hooks-signing-key.pub.pem`. The banner prints the public key. If the container runs in the background, read
the banner with `docker logs <container>`.

## Next steps

- [Build a code-hosting application](build/README.md) — build an application
  on Enroute.
- [Hook reference](reference/hooks.md) — every field of every hook, with
  payloads.
- [Concepts](README.md#concepts)
- [Configuration](operate/configuration.md)
- [Limitations](reference/limitations.md)
