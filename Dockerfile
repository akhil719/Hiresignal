FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Create data directory for SQLite
RUN mkdir -p /data
ENV DATABASE_URL=sqlite:////data/hiring_intel.db

EXPOSE 8000

CMD ["python", "main.py"]
