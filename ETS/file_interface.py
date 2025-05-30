import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):
        # Pastikan folder files ada
        if not os.path.exists('files'):
            os.makedirs('files')
        os.chdir('files/')
    
    def list(self, params=[]):
        try:
            filelist = glob('*.*')
            return dict(status='OK', data=filelist)
        except Exception as e:
            return dict(status='ERROR', data=str(e))
    
    def get(self, params=[]):
        try:
            if not params or len(params) == 0:
                return dict(status='ERROR', data='Nama file tidak disediakan')
            
            filename = params[0]
            if filename == '':
                return dict(status='ERROR', data='Nama file kosong')
            
            if not os.path.exists(filename):
                return dict(status='ERROR', data=f'File {filename} tidak ditemukan')
            
            with open(filename, 'rb') as fp:
                isifile = base64.b64encode(fp.read()).decode()
            
            return dict(status='OK', data_namafile=filename, data_file=isifile)
        except Exception as e:
            return dict(status='ERROR', data=str(e))
    
    def delete(self, params=[]):
        try:
            if not params or len(params) == 0:
                return dict(status='ERROR', data='Nama file tidak disediakan')
            
            filename = params[0]
            if filename == '':
                return dict(status='ERROR', data='Nama file kosong')
            
            if not os.path.exists(filename):
                return dict(status='ERROR', data=f'File {filename} tidak ditemukan')
            
            os.remove(filename)
            return dict(status='OK', data=f'File {filename} berhasil dihapus')
        except Exception as e:
            return dict(status='ERROR', data=str(e))
    
    def upload(self, params=[]):
        try:
            if not params or len(params) < 2:
                return dict(status='ERROR', data='Parameter upload tidak lengkap')
            
            filename = params[0]
            filedata_base64 = params[1]
            
            if filename == '':
                return dict(status='ERROR', data='Nama file kosong')
            
            if filedata_base64 == '':
                return dict(status='ERROR', data='Data file kosong')
            
            # Decode base64 data
            try:
                filedata = base64.b64decode(filedata_base64)
            except Exception as e:
                return dict(status='ERROR', data=f'Data base64 tidak valid: {str(e)}')
            
            # Write file
            with open(filename, 'wb') as f:
                f.write(filedata)
            
            return dict(status='OK', data=f'File {filename} berhasil diupload')
            
        except Exception as e:
            return dict(status='ERROR', data=str(e))

if __name__=='__main__':
    f = FileInterface()
    print(f.list())
    # print(f.get(['pokijan.jpg']))
