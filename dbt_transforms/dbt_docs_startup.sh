#!/bin/sh

dbt deps &

sleep 30

dbt docs generate &

sleep 30

dbt docs serve

# keep shell open to keep container alive
wait