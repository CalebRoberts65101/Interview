#!/bin/bash

go build cli.go

# sleep so db has time to come up
sleep 5

echo "Way to populate the data *****"
./cli --add_data true

# run commands
echo "Run default *****"
./cli

echo "Run with 4 workers *****"
./cli --workers=4

echo "Run with big query *****"
./cli --file=./input/query_params2.csv

echo "Run from stdin *****"
cat ./input/query_params.csv | ./cli --stdin