import sys
import os.path
import uuid
from glob import glob
from datetime import datetime
import base64

class HttpServer:
    def __init__(self):
        self.sessions = {}
        self.types = {}
        self.types['.pdf'] = 'application/pdf'
        self.types['.jpg'] = 'image/jpeg'
        self.types['.txt'] = 'text/plain'
        self.types['.html'] = 'text/html'

    def response(self, kode=404, message='Not Found', messagebody=bytes(), headers={}):
        tanggal = datetime.now().strftime('%c')
        resp = []
        resp.append("HTTP/1.0 {} {}\r\n".format(kode, message))
        resp.append("Date: {}\r\n".format(tanggal))
        resp.append("Connection: close\r\n")
        resp.append("Server: myserver/1.0\r\n")
        resp.append("Content-Length: {}\r\n".format(len(messagebody)))
        for kk in headers:
            resp.append("{}:{}\r\n".format(kk, headers[kk]))
        resp.append("\r\n")

        response_headers = ''
        for i in resp:
            response_headers = "{}{}".format(response_headers, i)

        if type(messagebody) is not bytes:
            messagebody = messagebody.encode()

        response = response_headers.encode() + messagebody
        return response

    def proses(self, data):
        requests = data.split("\r\n")
        baris = requests[0]
        all_headers = [n for n in requests[1:] if n != '']

        j = baris.split(" ")
        try:
            method = j[0].upper().strip()
            if method == 'GET':
                object_address = j[1].strip()
                if object_address == '/list':
                    return self.http_list()
                return self.http_get(object_address, all_headers)
            if method == 'POST':
                object_address = j[1].strip()
                if object_address == '/upload':
                    body = requests[-1]
                    filename = ""
                    for h in all_headers:
                        if h.lower().startswith("filename:"):
                            filename = h.split(":", 1)[1].strip()
                    return self.http_upload(filename, body)
                return self.http_post(object_address, all_headers)
            if method == 'DELETE':
                object_address = j[1].strip()
                return self.http_delete(object_address)
            else:
                return self.response(400, 'Bad Request', '', {})
        except IndexError:
            return self.response(400, 'Bad Request', '', {})

    def http_get(self, object_address, headers):
        files = glob('./*')
        thedir = './'
        if object_address == '/':
            return self.response(200, 'OK', 'Ini Adalah web Server percobaan', dict())

        if object_address == '/video':
            return self.response(302, 'Found', '', dict(location='https://youtu.be/katoxpnTf04'))

        if object_address == '/santai':
            return self.response(200, 'OK', 'santai saja', dict())

        object_address = object_address[1:]
        if thedir + object_address not in files:
            return self.response(404, 'Not Found', '', {})

        fp = open(thedir + object_address, 'rb')
        isi = fp.read()
        fp.close()

        fext = os.path.splitext(thedir + object_address)[1]
        content_type = self.types.get(fext, 'application/octet-stream')

        headers = {}
        headers['Content-type'] = content_type

        return self.response(200, 'OK', isi, headers)

    def http_post(self, object_address, headers):
        headers = {}
        isi = "kosong"
        return self.response(200, 'OK', isi, headers)

    def http_list(self):
        files = [f for f in os.listdir('.') if os.path.isfile(f)]
        isi = "\n".join(files)
        headers = {'Content-type': 'text/plain'}
        return self.response(200, 'OK', isi, headers)

    def http_upload(self, filename, body):
        if not filename:
            return self.response(400, 'Bad Request', '', {})
        try:
            filedata = base64.b64decode(body)
            with open(filename, 'wb') as f:
                f.write(filedata)
            return self.response(200, 'OK', f'File {filename} uploaded', {})
        except Exception as e:
            return self.response(500, 'Internal Server Error', str(e), {})

    def http_delete(self, object_address):
        filename = object_address.lstrip('/')
        if not os.path.exists(filename):
            return self.response(404, 'Not Found', '', {})
        try:
            os.remove(filename)
            return self.response(200, 'OK', f'File {filename} deleted', {})
        except Exception as e:
            return self.response(500, 'Internal Server Error', str(e), {})


if __name__ == "__main__":
    httpserver = HttpServer()