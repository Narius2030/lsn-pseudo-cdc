# --------------------------------------------------
# Base image
# --------------------------------------------------
FROM python:3.14-slim-bookworm

# --------------------------------------------------
# Environment variables
# --------------------------------------------------
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# --------------------------------------------------
# Install system dependencies + ODBC Driver 18
# --------------------------------------------------
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        ca-certificates \
    \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    \
    && apt-get update \
    \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
        unixodbc-dev \
    \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --------------------------------------------------
# Set working directory
# --------------------------------------------------
WORKDIR /app

# --------------------------------------------------
# Install the packaged application
# --------------------------------------------------
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir .

# --------------------------------------------------
# Entrypoint
# --------------------------------------------------
ENTRYPOINT ["sqlserver-cdc-s3"]
