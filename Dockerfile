FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY raft.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

COPY config.py storage.py raft_node.py client.py ./

# Node id and peer list are supplied at run time (see docker-compose.yml).
ENV NODE_ID=1
CMD ["sh", "-c", "python raft_node.py --id ${NODE_ID}"]
