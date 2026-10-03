import socket
import sys

host = '100.104.93.92'
port = 8000

try:
    s = socket.socket()
    s.settimeout(5)
    result = s.connect_ex((host, port))
    s.close()
    if result == 0:
        print("CONNECTED")
        sys.exit(0)
    else:
        print(f"FAILED: {result}")
        sys.exit(1)
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
