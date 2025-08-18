# This Dockerfile is exclusively for smithery.ai.

FROM rockylinux:9.3

ARG PYTHON_VERSION=3.11

# Base dependencies
RUN dnf install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-pip git && dnf clean all \
        && update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 1000 --slave /usr/bin/pip pip /usr/bin/pip${PYTHON_VERSION}

WORKDIR /app

COPY . /app

# Fetch git tags and update version
RUN git fetch --tags origin && \
    LATEST_TAG=$(git tag | tail -n 1) && \
    echo "LATEST_TAG='$LATEST_TAG'" && \
    sed -i "s|^version = \".*\"|version = \"${LATEST_TAG}\"|" pyproject.toml && \
    rm -rf .git .github img test .venv

RUN pip install --no-cache-dir --upgrade wheel

RUN pip install --no-cache-dir --upgrade 'uv>=0.8.5'

RUN uv sync

RUN pip install .

ENV FASTMCP_HOST=0.0.0.0
ENV MCP_SERVER_PORT=18000
EXPOSE ${MCP_SERVER_PORT}

CMD ["python", "-m", "mcp_airflow_api.airflow_api"]