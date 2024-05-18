FROM python:3.11.9-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1 

# Update package lists and install curl
RUN apt-get update && \
    # apt-get install -y curl && \
    # apt-get install -y procps && \
    rm -rf /var/lib/apt/lists/*

# Set working directory early to ensure following paths are relative to /app
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY . .

# # Ensure the start script is executable
# RUN chmod +x prefect_startup.sh

# Specify the command to run on container start
# CMD ["tail", "-f", "/dev/null"]
CMD ["./prefect_startup.sh"]