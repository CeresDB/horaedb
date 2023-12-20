#!/usr/bin/env bash

set -e

ROOT=`pwd`
# For compatibility in macos, so convert to milliseconds by adding 3 zeros.
NOW=`date +%s000`
BINARY_PATH=${ROOT}/../../target/debug/horaedb-server
SERVER_HTTP_ENDPOINT=127.0.0.1:5440

CONFIG_FILE=${ROOT}/../../docs/minimal.toml
if [ ${1} == 'shard_based' ]; then
    CONFIG_FILE=${ROOT}/../config/shard-based-recovery.toml
fi

echo "Run with config: ${CONFIG_FILE}"
echo "First check..."
nohup ${BINARY_PATH} --config ${CONFIG_FILE} &
sleep 10
python3 ./check.py -ts ${NOW} -i

echo "Restart and check..."
killall horaedb-server | true
nohup ${BINARY_PATH} --config ${CONFIG_FILE} &
sleep 10
python3 ./check.py -ts ${NOW}

echo "Flush, restart and check..."
curl -XPOST ${SERVER_HTTP_ENDPOINT}/debug/flush_memtable
echo "\nFlush finish..."
killall horaedb-server | true
nohup ${BINARY_PATH} --config ${CONFIG_FILE} &
sleep 10
python3 ./check.py -ts ${NOW}
echo "All finish..."
