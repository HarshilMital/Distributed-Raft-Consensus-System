#!/usr/bin/env bash
# Create a virtualenv, install dependencies and regenerate the gRPC stubs.
set -euo pipefail

cd "$(dirname "$0")"

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

echo
echo "Done. Activate the environment with:  source .venv/bin/activate"
echo "Then start a five node cluster with:  ./run_cluster.sh start"
