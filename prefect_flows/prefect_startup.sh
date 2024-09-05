#!/bin/sh
prefect config set PREFECT_SERVER_API_HOST="0.0.0.0" &

sleep 5

prefect config set PREFECT_API_URL="http://0.0.0.0:4200/api" &

sleep 5

prefect server start &

sleep 30

prefect worker start -p 'my-pool' &

sleep 30

python prefect_flows/prefect_deployment.py

# keep shell open to keep container alive
wait
