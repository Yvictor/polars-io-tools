FROM python:3.13-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        curl \
        odbc-mariadb \
        odbc-postgresql \
        unixodbc \
    && curl --silent --show-error --location \
        https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb \
        --output /tmp/packages-microsoft-prod.deb \
    && dpkg --install /tmp/packages-microsoft-prod.deb \
    && rm /tmp/packages-microsoft-prod.deb \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install --yes --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Import the source tree directly. Installing the runtime dependencies without
# building the package avoids requiring a Rust toolchain in the benchmark image.
RUN pip install --no-cache-dir \
    arrow_odbc \
    boto3 \
    cloudpickle \
    connectorx==0.4.5 \
    deltalake \
    duckdb \
    narwhals \
    numpy \
    orjson \
    packaging \
    pandas \
    polars \
    portion \
    pyarrow \
    pyodbc \
    pydantic \
    ray \
    requests \
    sqlglot \
    tqdm

COPY . /workspace

ENTRYPOINT ["python", "benchmarks/benchmark_sql_backends.py"]
