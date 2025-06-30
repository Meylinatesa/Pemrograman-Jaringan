import socket
import threading
from datetime import datetime

def handle_client(client_socket, client_address):
    print(f"[INFO] Terhubung dengan {client_address}")
    try:
        while True:
            data = client_socket.recv(1024).decode("utf-8")

            if not data:
                break

            print(f"[REQUEST dari {client_address}]: {repr(data)}")

            data = data.strip()

            if data.upper() == "QUIT":
                print(f"[INFO] {client_address} keluar.")
                break

            if data.startswith("TIME"):
                now = datetime.now()
                current_time = now.strftime("%H:%M:%S")
                response = f"JAM {current_time}\r\n"
                client_socket.send(response.encode("utf-8"))
            else:
                response = "INVALID REQUEST\r\n"
                client_socket.send(response.encode("utf-8"))
    finally:
        client_socket.close()

def main():
    host = "0.0.0.0"
    port = 45000

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    print(f"[SERVER] Listening on {host}:{port}...")

    while True:
        client_sock, client_addr = server.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_sock, client_addr))
        client_thread.start()

if __name__ == "__main__":
    main()