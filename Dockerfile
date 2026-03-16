FROM thehale/python-poetry:2.3.1-py3.13-slim

RUN mkdir /code
COPY pyproject.toml /code/pyproject.toml

WORKDIR /code
RUN poetry install --no-root
