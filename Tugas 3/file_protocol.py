import json
import logging
import shlex

from file_interface import FileInterface

"""
* class FileProtocol bertugas untuk memproses 
data yang masuk, dan menerjemahkannya apakah sesuai dengan
protokol/aturan yang dibuat

* data yang masuk dari client adalah dalam bentuk bytes yang 
pada akhirnya akan diproses dalam bentuk string

* class FileProtocol akan memproses data yang masuk dalam bentuk
string
"""



class FileProtocol:
    def __init__(self):
        self.file = FileInterface()
    def proses_string(self,string_datamasuk=''):
        logging.warning(f"string diproses: {string_datamasuk}")
        try:
            parts = shlex.split(string_datamasuk)  # JANGAN .lower()
            if not parts:
                return json.dumps(dict(status='ERROR', data='perintah kosong'))

            command = parts[0].lower()  # hanya command yang di-lowercase
            params = parts[1:]

            logging.warning(f"memproses request: {command}")

            # Coba panggil fungsi sesuai command
            cl = getattr(self.file, command)(params)
            return json.dumps(cl)

        except Exception as e:
            logging.error(f"Terjadi error saat memproses: {str(e)}")
            return json.dumps(dict(status='ERROR', data='request tidak dikenali'))


if __name__=='__main__':
    #contoh pemakaian
    fp = FileProtocol()
    print(fp.proses_string("LIST"))
    print(fp.proses_string("GET pokijan.jpg"))
