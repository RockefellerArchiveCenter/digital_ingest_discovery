FROM python:3.11-alpine AS base
WORKDIR /code
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src src
COPY rac_schemas/schemas schemas

FROM base AS test
COPY test_requirements.txt .coveragerc ./
RUN pip install -r test_requirements.txt
COPY tests tests

FROM base AS build
CMD [ "python", "-m", "src.discover_packages" ]