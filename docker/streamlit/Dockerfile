FROM python:3.11.9-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1 

# Update package lists and install curl
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

# Set working directory early to ensure following paths are relative to /noaa_app
WORKDIR /noaa_app

# Install dependencies
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY . .

# Specify the command to run on container start
ENTRYPOINT ["streamlit", "run", "app/app.py"]