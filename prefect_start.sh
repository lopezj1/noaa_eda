#!/bin/sh

# Function to check if Prefect server is ready
prefect_server_ready() {
    response=$(curl -s -o /dev/null -w "%{http_code}" http://host.docker.internal:4200/health)
    if [ $response -eq 200 ]; then
        return 0 # Server is ready
    else
        return 1 # Server is not ready
    fi
}

# Function to check if Prefect agent is ready
prefect_agent_ready() {
    # Check if Prefect agent process is running
    if pgrep -x "prefect" > /dev/null; then
        return 0 # Agent is running
    else
        return 1 # Agent is not running
    fi
}

# Start Prefect server
echo "Starting Prefect server..."
prefect config set PREFECT_API_URL='http://host.docker.internal:4200/api' && prefect server start &
agent_pid=$!

# Wait for Prefect server to become ready
echo "Waiting for Prefect server to start..."
while ! prefect_server_ready; do
    sleep 5
done
echo "Prefect server is ready!"

# Wait for Prefect server process to finish
wait $agent_pid

# Start Prefect agent in a subshell
echo "Starting Prefect agent..."
prefect agent start --pool 'default-agent-pool' &
agent_pid=$!

# Wait for Prefect agent to become ready
echo "Waiting for Prefect agent to start..."
while ! prefect_agent_ready; do
    sleep 5
done
echo "Prefect agent is ready!"

# Wait for Prefect agent process to finish
wait $agent_pid

# Deploy flows
echo "Deploying flows..."
python elt/prefect_deployment.py
