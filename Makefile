.PHONY: test itest venv clean default e2e

default: venv

test:
	tox

itest:
	tox -e itest

e2e:
	tox -e e2e

venv:
	tox -e venv

clean:
	git clean -fdX
