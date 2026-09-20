FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies in a separate cached layer.
COPY requirements.txt .

RUN python -m pip install \
    --no-cache-dir \
    -r requirements.txt

# Create a non-root runtime user.
RUN useradd \
    --create-home \
    --uid 10001 \
    appuser

# Copy only application and administration code.
COPY --chown=appuser:appuser app ./app
COPY --chown=appuser:appuser scripts ./scripts

USER appuser

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]