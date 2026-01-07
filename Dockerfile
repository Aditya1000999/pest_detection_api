FROM python:3.10-slim

# Install system dependencies untuk OpenCV
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first (for Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8080

# Set PORT environment variable
ENV PORT=8080

# ✅ UBAH --workers 2 MENJADI --workers 1
# Start application with gunicorn (single worker for MQTT compatibility)
CMD gunicorn app:app --bind 0.0.0.0:8080 --workers 1 --timeout 120 --log-level info --access-logfile - --error-logfile -