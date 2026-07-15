#!/bin/bash
#-------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#-------------------------------------------------------------

export SYSDS_QUIET=1
export SYSTEMDS_STANDALONE_OPTS="-Xmx8g -Xms1g -Xmn256m"

# use THIS repo's built systemds, not whatever 'systemds' is on PATH
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export SYSTEMDS_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SYSTEMDS="${SYSTEMDS_BIN:-$SYSTEMDS_ROOT/bin/systemds}"
if [[ ! -x "$SYSTEMDS" ]]; then
    echo "ERROR: launcher not found/executable: $SYSTEMDS" >&2
    exit 1
fi
echo "Using SYSTEMDS_ROOT=$SYSTEMDS_ROOT"

# stop the federated worker and drop the regenerated scratch on exit (CLEAN=0 keeps ./tmp)
CLEAN=${CLEAN:-1}
cleanup() {
    [[ -n "${WORKER_PID:-}" ]] && kill "$WORKER_PID" 2>/dev/null
    [[ "$CLEAN" == "1" ]] && rm -rf ./tmp
}
trap cleanup EXIT

"$SYSTEMDS" WORKER 50505 \
    --config ./SystemDS-config.xml \
    --exec singlenode \
    -stats &
WORKER_PID=$!

ROWS=${ROWS:-8900}
COLS=${COLS:-30000}
MIN=${MIN:-0}
MAX=${MAX:-1}
DATA=${DATA:-"./tmp/X"}
"$SYSTEMDS" -f ./genRandData.dml \
    --nvargs rows=$ROWS cols=$COLS min=$MIN max=$MAX target=$DATA

NUMFED=1
echo "{\"data_type\": \"list\", \"rows\": $NUMFED, \"cols\": 1, \"format\": \"text\"}" > ./tmp/hosts.mtd
if [ ! -d ./tmp/hosts ]; then mkdir -p ./tmp/hosts ; fi
echo "localhost:50505" > ./tmp/hosts/0_null
echo "{\"data_type\": \"scalar\", \"value_type\": \"string\", \"format\": \"text\"}" > ./tmp/hosts/0_null.mtd
FED_DATA="${DATA}_fed.json"
"$SYSTEMDS" -f ./splitAndMakeFederated.dml \
    --config ./SystemDS-config.xml \
    --nvargs data=$DATA nSplit=$NUMFED transposed=FALSE \
    target=$FED_DATA hosts="./tmp/hosts" fmt="csv" hostOffset=0

SCRIPT=${SCRIPT:-"./ewSumIx90.dml"}
"$SYSTEMDS" -f $SCRIPT \
      --exec singlenode \
      --config ./SystemDS-config.xml \
      --stats \
      --nvargs data=$FED_DATA target="./tmp/Z"

# success check before the exit trap wipes ./tmp
if [[ -d ./tmp/Z ]]; then
    echo "RESULT: Z written ($(du -sh ./tmp/Z | cut -f1)) - federated >2GB transfer OK"
else
    echo "RESULT: Z missing - run FAILED" >&2
fi
