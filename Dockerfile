FROM thehale/python-poetry:2.3.1-py3.13-slim

RUN mkdir /code
COPY pyproject.toml /code/pyproject.toml

WORKDIR /code
RUN POETRY_VIRTUALENVS_CREATE=false poetry install --no-root

RUN mkdir /test
RUN mkdir /output

RUN chown 1000:1000 /test
RUN chown 1000:1000 /output

USER 1000
