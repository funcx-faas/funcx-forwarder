# CONTRIBUTING

## Tool Requirements

- `make`
- `tox`

Install `tox` with `pipx install tox`

### Recommended

- `pre-commit`
- Docker

Install `scriv` and `pre-commit` with

    pipx install scriv
    pipx install pre-commit

## Linting & Testing

Testing and linting are run under [tox](https://tox.readthedocs.io/en/latest/).

### Running Tests

Tests require a local redis instance to be up and running.

With that in place, run tox:

    tox

### Local Redis in Docker

The easiest way to get a local redis instance is to run a docker container:

.. code-block:: bash

    docker run -d -p 6379:6379 --name funcx-forwarder-test-redis redis

### Optional, but recommended, linting setup

For the best development experience, we recommend setting up linting and
autofixing integrations with your editor and git.

We use [pre-commit](https://pre-commit.com/) to automatically run linters and fixers.
Install `pre-commit` and then run

    $ pre-commit install

to setup the hooks.

The configured linters and fixers can be seen in `.pre-commit-config.yaml`.
