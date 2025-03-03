# Use the official Python image.
FROM python:3.12

# Setup env variables.
ENV TMPDIR=/app/tmpdir
ENV ARROYO_BACKEND_DEVELOPMENT=true

# Set the working directory inside the container.
WORKDIR /app

# Prepare the temp dir.
RUN mkdir tmpdir

# Install uv.
RUN pip install --no-cache-dir uv

# Copy only pyproject.toml first for caching efficiency.
COPY pyproject.toml ./

# Install dependencies.
RUN uv sync

# Copy the rest of the application files/
COPY . .

# Run fasapi.
CMD ["uv", "run", "--", "fastapi", "run", "main.py"]

# Expose FastAPI's default port.
EXPOSE 8000

