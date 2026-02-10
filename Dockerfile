FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app.py .

# Expose port
EXPOSE 5000

# Environment variables (set these in DigitalOcean)
ENV POLYMARKET_API_KEY=""
ENV POLYMARKET_API_SECRET=""
ENV DEMO_MODE="false"

# Run the bot
CMD ["python", "app.py"]
