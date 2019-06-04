.PHONY: test itest venv clean default

default: venv

test:
	tox

itest:
	tox -e itest

venv:
	tox -e venv

clean:
	git clean -fdX
