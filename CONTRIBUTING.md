# Contributing

Bug reports and pull requests are welcome. Keep each submission to one topic, and describe what
you did, what you expected and what happened instead.

## Development setup

The project uses [mise](https://mise.jdx.dev) for the toolchain and [uv](https://docs.astral.sh/uv/)
for dependencies. The tests need a running PostgreSQL server.

```shell
mise trust
mise run bootstrap   # install dependencies into .venv
createdb eventstream_pg
mise run test        # pytest with coverage
mise run test-all    # the Python and Django matrix via tox
mise run lint        # pre-commit, including basedpyright in strict mode
```

`tests/settings.py` connects to the local Unix socket as the current user. Set `POSTGRES_HOST`,
`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT` or `POSTGRES_DB` to use another server.

## Changes

- Write a failing test first, then the change.
- Use [Conventional Commits](https://www.conventionalcommits.org/) for commit messages.
- Add a line to the `## Unreleased` section of `CHANGELOG.md` for every user-visible change.

## Releases

Rename `## Unreleased` in `CHANGELOG.md` to the version and date, commit, and push a tag such as
`v0.1.0`. The release workflow publishes to TestPyPI, then PyPI, and creates a signed GitHub
release with that changelog section.
