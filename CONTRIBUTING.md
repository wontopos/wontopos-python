# Contributing

This repository is a mirror. The Python client is developed alongside the Python
and Rust clients in a private repository and copied here when a release is cut, so it
carries one commit per release rather than the working history.

Issues and pull requests are read. A change lands privately first and arrives here
with the next release, which means an accepted suggestion appears inside a release
commit rather than as your own — say so on the issue if that matters and it will be
credited in the changelog instead.

## The three clients release together

Python, TypeScript and Rust ship as one release: same version, same surface, same day.
Raising one language alone is how a method ends up existing in two clients and not the
third, and a caller reading one README cannot tell which one they have. That is also
why a fix accepted here is not released from here.

## Tests

Offline — local mock servers, no network and no API key.

```bash
python3 -m unittest discover -s tests
```

## Changelog

`CHANGELOG.md` is shared by the three clients and is identical in all three
repositories. One entry per release, covering all of them.
