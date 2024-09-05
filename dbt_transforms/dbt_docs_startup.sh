#!/bin/sh

dbt deps &

sleep 30

dbt docs generate &

sleep 30

dbt docs serve --host '0.0.0.0'

# keep shell open to keep container alive
wait
