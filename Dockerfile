FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY pyproject.toml /app/
RUN pip install --no-cache-dir /app/

# Copy the package
COPY dp_mqtt/ /app/dp_mqtt/

# Create config directory
RUN mkdir -p /config

# Set default config path
ENV CONFIG_PATH=/config/config.yaml

# Disable Python output buffering for Docker logs
ENV PYTHONUNBUFFERED=1

# Expose MQTT port
EXPOSE 1883

# Run the broker
CMD python -u -m dp_mqtt --config $CONFIG_PATH --debug
