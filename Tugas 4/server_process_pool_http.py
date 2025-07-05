from socket import *
import socket
import time
import sys
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from http import HttpServer

httpserver = HttpServer()

#untuk menggunakan processpoolexecutor, karena tidak mendukung subclassing pada process,
#maka class ProcessTheClient dirubah dulu menjadi function, tanpda memodifikasi behaviour didalamnya

# ...existing code...
def ProcessTheClient(connection, address):
    rcv = b""
    headers_ended = False
    content_length = 0
    while True:
        try:
            data = connection.recv(1024*1024)
            if data:
                #merubah input dari socket (berupa bytes) ke dalam string
				#agar bisa mendeteksi \r\n
                d = data
                rcv=rcv+d
                if not headers_ended:
                    if b"\r\n\r\n" in rcv:
                        headers_ended = True
                        header, rest = rcv.split(b"\r\n\r\n", 1)
                        header_str = header.decode()
                        for line in header_str.split("\r\n"):
                            if line.lower().startswith("content-length:"):
                                content_length = int(line.split(":", 1)[1].strip())
                        body = rest
                        while len(body) < content_length:
                            more = connection.recv(1024*1024)
                            if not more:
                                break
                            body += more
                        full_request = header + b"\r\n\r\n" + body
                        hasil = httpserver.proses(full_request.decode(errors='ignore'))
                        hasil = hasil + "\r\n\r\n".encode()
                        connection.sendall(hasil)
                        connection.close()
                        return
                else:
                    #end of command, proses string
					#logging.warning("data dari client: {}" . format(rcv))
                    hasil = httpserver.proses(rcv.decode(errors='ignore'))
                    #hasil akan berupa bytes
					#untuk bisa ditambahi dengan string, maka string harus di encode
                    hasil = hasil + "\r\n\r\n".encode()
                    #logging.warning("balas ke  client: {}" . format(hasil))
					#hasil sudah dalam bentuk bytes
                    connection.sendall(hasil)
                    connection.close()
                    return
            else:
                break
        except OSError as e:
            pass
    connection.close()
    return



def Server():
	the_clients = []
	my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

	my_socket.bind(('0.0.0.0', 8889))
	my_socket.listen(1)

	with ProcessPoolExecutor(20) as executor:
		while True:
				connection, client_address = my_socket.accept()
				#logging.warning("connection from {}".format(client_address))
				p = executor.submit(ProcessTheClient, connection, client_address)
				the_clients.append(p)
				#menampilkan jumlah process yang sedang aktif
				jumlah = ['x' for i in the_clients if i.running()==True]
				print(jumlah)





def main():
	Server()

if __name__=="__main__":
	main()