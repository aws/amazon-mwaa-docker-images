import socket

UDP_IP = "0.0.0.0"
UDP_PORT = 8125

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

print(f"Listening for StatsD metrics on {UDP_IP}:{UDP_PORT}")

try:
    while True:
        data, addr = sock.recvfrom(1024)
        print(f"Received message: {data.decode()} from {addr}")
except KeyboardInterrupt:
    sock.close()
    print("Server stopped.")
