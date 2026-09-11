FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install git (needed for git dependencies) and uv
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install uv

# Copy dependency definitions first (for layer caching)
COPY pyproject.toml uv.lock README.md ./

# Create non-root user with home directory for uv cache
RUN useradd --create-home --shell /bin/false appuser

# Install dependencies (before copying source for better caching)
# Need src/ stub for uv sync to work with editable install
COPY src/actions/__init__.py ./src/actions/__init__.py
RUN uv sync --frozen --no-dev && chown -R appuser:appuser /app/.venv

COPY --chown=appuser:appuser src/ ./src/

# The subscriptions table ships with the image as a default, but is expected to
# be mounted over at runtime so routing changes do not need a rebuild.
COPY --chown=appuser:appuser config/ ./config/
VOLUME /app/config

USER appuser

CMD ["uv", "run", "python", "-m", "actions.services.router"]
