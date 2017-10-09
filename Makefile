.PHONY: test venv clean

test: requirements.txt requirements-dev.txt
	tox

venv: requirements.txt requirements-dev.txt
	tox -e venv

clean:
	git clean -fdX
