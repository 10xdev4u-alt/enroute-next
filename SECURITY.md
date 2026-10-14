# Security

## Supported versions

Enroute is `0.x`. Only the latest release receives security fixes. There is
no backwards compatibility between releases, so applying one means starting
clean and pushing the repositories again.

## Reporting a vulnerability

Report privately, through this repository's private vulnerability reporting
(**Security** then **Report a vulnerability**). Do not open a public issue for
a vulnerability.

If private reporting is unavailable, open a minimal issue that asks for a
private channel and nothing else. Send the details once one exists.

Include the version, the configuration with any credential removed, and the
smallest reproduction you have. Expect an acknowledgement within five working
days.

## Read this before serving traffic

[docs/operate/security.md](docs/operate/security.md) states what a node does
and does not do. In short: it terminates no TLS, it authenticates no API
caller, it does not encrypt data at rest, and the hook endpoint is the only
point where authorization happens. The same page states that the hook signing
key identifies Enroute to the application and does not authenticate the
caller.
