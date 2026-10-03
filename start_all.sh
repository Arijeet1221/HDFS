#!/bin/bash

echo "Starting Mini HDFS System..."
echo ""

echo "[0/5] Starting Kafka event broker..."
docker compose -f kafka/docker-compose.yml up -d
sleep 3

echo "[1/4] Starting NameNode..."
gnome-terminal -- python namenode/namenode.py &
sleep 2

echo "[2/4] Starting DataNode 1..."
gnome-terminal -- python datanode1/datanode1.py &
sleep 2

echo "[3/4] Starting DataNode 2..."
gnome-terminal -- python datanode2/datanode2.py &
sleep 2

echo "[4/5] Starting Client/Web Dashboard..."
gnome-terminal -- python client/client.py &
sleep 2

echo "[5/5] Starting DataNode 3..."
gnome-terminal -- python datanode3/datanode3.py &

echo ""
echo "All services started successfully!"
echo ""
echo "Access the web dashboard at: http://127.0.0.1:5000"
echo ""
