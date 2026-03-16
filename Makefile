.PHONY: test itest venv clean default e2e release

default: venv

test:
	mypy backuppy --ignore-missing-imports
	docker build . -t test_image
	docker run -it --init -v `pwd`:/code test_image /bin/bash -c "\
		poetry run coverage erase && \
		poetry run coverage run -m pytest tests && \
		poetry run coverage report --show-missing --fail-under 90"

itest:
	pytest -svvx itests

e2e:
	e2e/e2e.sh

clean:
	git clean -fdX

release:
	@git diff-index HEAD --quiet || (echo "ERROR: git index is not clean" && exit 1)
	echo "__version__ = '$(VERSION)'" > backuppy/__init__.py
	git commit -a -m "Release v$(VERSION)" && git tag v$(VERSION)
	@echo "Now push to origin to release"

# shameless hack stolen from
# https://stackoverflow.com/questions/2214575/passing-arguments-to-make-run
# If the first argument is "release"...
ifeq (release,$(firstword $(MAKECMDGOALS)))
  # use the next argument as the version
  VERSION := $(wordlist 2,3,$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(VERSION):;@:)
endif
