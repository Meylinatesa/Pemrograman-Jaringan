import sys
import socket
import json
import logging
import ssl
import os
import base64

server_address = ('172.16.16.101', 8889)

def make_socket(destination_address='172.16.16.101', port=24):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (destination_address, port)
        logging.warning(f"connecting to {server_address}")
        sock.connect(server_address)
        return sock
    except Exception as ee:
        logging.warning(f"error {str(ee)}")


def make_secure_socket(destination_address='172.16.16.101', port=10000):
    try:
        # get it from https://curl.se/docs/caextract.html

        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_verify_locations(os.getcwd() + '/domain.crt')

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (destination_address, port)
        logging.warning(f"connecting to {server_address}")
        sock.connect(server_address)
        secure_socket = context.wrap_socket(sock, server_hostname=destination_address)
        logging.warning(secure_socket.getpeercert())
        return secure_socket
    except Exception as ee:
        logging.warning(f"error {str(ee)}")



def send_command(command_str, is_secure=False):
    alamat_server = server_address[0]
    port_server = server_address[1]
    #    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # gunakan fungsi diatas
    if is_secure == True:
        sock = make_secure_socket(alamat_server, port_server)
    else:
        sock = make_socket(alamat_server, port_server)

    logging.warning(f"connecting to {server_address}")
    try:
        logging.warning(f"sending message ")
        sock.sendall(command_str.encode())
        logging.warning(command_str)
        # Look for the response, waiting until socket is done (no more data)
        data_received = ""  # empty string
        while True:
            # socket does not receive all data at once, data comes in part, need to be concatenated at the end of process
            data = sock.recv(2048)
            if data:
                # data is not empty, concat with previous content
                data_received += data.decode()
                if "\r\n\r\n" in data_received:
                    break
            else:
                # no more data, stop the process by break
                break
        # at this point, data_received (string) will contain all data coming from the socket
        # to be able to use the data_received as a dict, need to load it using json.loads()
        hasil = data_received
        logging.warning("data received from server:")
        return hasil
    except Exception as ee:
        logging.warning(f"error during data receiving {str(ee)}")
        return False

def list_files(is_secure=False):
    cmd = "GET /list HTTP/1.0\r\n\r\n"
    hasil = send_command(cmd, is_secure)
    print("Daftar file di server:\n", hasil)

def upload_file(filepath, is_secure=False):
    filename = os.path.basename(filepath)
    with open(filepath, 'rb') as f:
        filedata = f.read()
    encoded_data = base64.b64encode(filedata).decode('ascii')
    headers = f"POST /upload HTTP/1.0\r\nFilename: {filename}\r\nContent-Length: {len(encoded_data)}\r\n\r\n"
    cmd = headers + encoded_data
    hasil = send_command(cmd, is_secure)
    print("Upload response:\n", hasil)

def delete_file(filename, is_secure=False):
    cmd = f"DELETE /{filename} HTTP/1.0\r\n\r\n"
    hasil = send_command(cmd, is_secure)
    print("Delete response:\n", hasil)
    
def tampilkan_menu():
    while True:
        print("\n=== PILIHAN UTAMA ===")
        print("1. Tampilkan file di server")
        print("2. Unggah file ke server")
        print("3. Hapus file dari server")
        print("4. Selesai")
        pilihan_user = input("Silakan pilih opsi [1-4]: ").strip()
        if pilihan_user == "1":
            list_files(is_secure=False)
        elif pilihan_user == "2":
            path_file = input("Masukkan lokasi file yang ingin diunggah: ").strip()
            if os.path.isfile(path_file):
                upload_file(path_file, is_secure=False)
            else:
                print("File tidak ditemukan di lokasi tersebut.")
        elif pilihan_user == "3":
            nama_file = input("Masukkan nama file yang ingin dihapus di server: ").strip()
            delete_file(nama_file, is_secure=False)
        elif pilihan_user == "4":
            print("Program selesai. Terima kasih.")
            break
        else:
            print("Input tidak dikenali. Coba lagi.")


#> GET / HTTP/1.1
#> Host: www.its.ac.id
#> User-Agent: curl/8.7.1
#> Accept: */*
#>

if __name__ == '__main__':
#     cmd = f"""GET /rfc/rfc2616.txt HTTP/1.1
# Host: www.ietf.org
# User-Agent: myclient/1.1
# Accept: */*

# """
#     hasil = send_command(cmd, is_secure=True)
#     print(hasil)
    tampilkan_menu()