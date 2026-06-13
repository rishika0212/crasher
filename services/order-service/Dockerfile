FROM python:3.11-slim

# Install system dependencies required for stress and traffic shaping chaos scenarios
RUN apt-get update && \
    apt-get install -y --no-install-recommends iproute2 stress-ng procps curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
