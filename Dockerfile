# This Dockerfile is exclusively for smithery.ai.

FROM rockylinux:9.3

ARG PYTHON_VERSION=3.11

# Base dependencies
RUN dnf install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-pip git which && dnf clean all \
        && update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 1000 --slave /usr/bin/pip pip /usr/bin/pip${PYTHON_VERSION}

WORKDIR /app

COPY . /app

# Fetch git tags and update version
RUN git fetch --tags origin && \
    LATEST_TAG=$(git tag | tail -n 1) && \
    echo "LATEST_TAG='$LATEST_TAG'" && \
    sed -i "s|^version = \".*\"|version = \"${LATEST_TAG}\"|" pyproject.toml && \
    rm -rf .git

RUN pip install --no-cache-dir --upgrade wheel

RUN pip install --no-cache-dir --upgrade 'uv>=0.8.5'

RUN uv sync

RUN pip install .

# Ensure the binary is accessible and test basic functionality  
RUN which mcp-airflow-api && mcp-airflow-api --help

# smithery.ai will provide PORT environment variable dynamically
# The application auto-detects PORT env var for HTTP transport

# Start the MCP server (will auto-detect PORT env var for HTTP transport)
CMD ["mcp-airflow-api"]