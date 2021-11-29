.PHONY: lint
lint:
	tox -e lint,mypy

.PHONY: test
test:
	tox

.PHONY: py-safety
py-safety:
	tox -e safety
