#!/bin/sh

prefect server start &

sleep 30

prefect agent start --pool 'default-agent-pool' &

sleep 30

python prefect_flows/prefect_deployment.py

# keep shell open to keep container alive
wait