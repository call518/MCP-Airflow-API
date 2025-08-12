# This Dockerfile is exclusively for smithery.ai.

FROM rockylinux:9.3

ARG PYTHON_VERSION=3.11

RUN sleep 120

# Base dependencies
RUN dnf install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-pip git && dnf clean all \
        && update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 1000 --slave /usr/bin/pip pip /usr/bin/pip${PYTHON_VERSION}

RUN pip install --upgrade \
        'uv>=0.8.5' \
        'mcpo>=0.0.17' \
        'fastmcp>=2.11.1' \
        'aiohttp>=3.12.0' \
        'mcp-airflow-api'

CMD ["mcp-airflow-api"]
