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
    
    def proses_string(self, string_datamasuk=''):
        logging.warning(f"string diproses: {string_datamasuk[:100]}...")  # Log hanya 100 karakter pertama
        
        try:
            # Untuk command UPLOAD, kita perlu parsing khusus karena data base64 sangat besar
            string_datamasuk = string_datamasuk.strip()
            
            if string_datamasuk.upper().startswith('UPLOAD'):
                # Parsing khusus untuk UPLOAD: UPLOAD filename base64_data
                parts = string_datamasuk.split(' ', 2)  # Split maksimal menjadi 3 bagian
                if len(parts) < 3:
                    return json.dumps(dict(status='ERROR', data='Format UPLOAD tidak valid'))
                
                command = parts[0].lower()
                filename = parts[1]
                filedata_base64 = parts[2]
                params = [filename, filedata_base64]
                
            else:
                # Parsing normal untuk command lainnya
                parts = shlex.split(string_datamasuk)
                if not parts:
                    return json.dumps(dict(status='ERROR', data='perintah kosong'))
                
                command = parts[0].lower()
                params = parts[1:]
            
            logging.warning(f"memproses request: {command}")
            
            # Coba panggil fungsi sesuai command
            if not hasattr(self.file, command):
                return json.dumps(dict(status='ERROR', data=f'Command {command} tidak dikenali'))
            
            method = getattr(self.file, command)
            cl = method(params)
            return json.dumps(cl)
            
        except Exception as e:
            logging.error(f"Terjadi error saat memproses: {str(e)}")
            return json.dumps(dict(status='ERROR', data='request tidak dikenali'))

if __name__=='__main__':
    #contoh pemakaian
    fp = FileProtocol()
    print(fp.proses_string("LIST"))
    print(fp.proses_string("GET pokijan.jpg"))
