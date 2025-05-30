import socket
import json
import base64
import logging
import os
import time
import threading

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

server_address = ('127.0.0.1', 6666)

BUFFER_SIZE = 1048576
SOCKET_TIMEOUT = 600

_thread_local = threading.local()

def create_connection():
    """Create a new socket connection with optimal settings"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Set socket options for better performance
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4194304)  # 4MB receive buffer
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4194304)  # 4MB send buffer
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)     # Disable Nagle's algorithm
    sock.settimeout(SOCKET_TIMEOUT)
    
    return sock

def send_command(command_str="", max_retries=3):
    """Send command with retry logic and robust error handling"""
    thread_id = threading.current_thread().ident
    
    for attempt in range(max_retries):
        sock = None
        try:
            logging.info(f"[Thread {thread_id}] Attempt {attempt + 1}: Sending command ({len(command_str)} bytes)")
            
            # Create new connection for each attempt
            sock = create_connection()
                            
            sock.connect(server_address)
            logging.info(f"[Thread {thread_id}] Connected to {server_address}")
            
            # Send command in chunks
            command_bytes = command_str.encode('utf-8')
            bytes_sent = 0
            total_bytes = len(command_bytes)
            
            start_time = time.time()
            
            while bytes_sent < total_bytes:
                try:
                    remaining = total_bytes - bytes_sent
                    chunk_size = min(BUFFER_SIZE, remaining)
                    chunk = command_bytes[bytes_sent:bytes_sent + chunk_size]
                    
                    sent = sock.send(chunk)
                    if sent == 0:
                        raise ConnectionError("Socket connection broken")
                    
                    bytes_sent += sent
                    
                    # Progress logging for large uploads
                    if total_bytes > 10 * 1024 * 1024:  # Only for files > 10MB
                        progress_mb = bytes_sent / (1024 * 1024)
                        total_mb = total_bytes / (1024 * 1024)
                        if bytes_sent % (5 * 1024 * 1024) == 0:  # Every 5MB
                            elapsed = time.time() - start_time
                            speed = bytes_sent / elapsed / 1024 / 1024 if elapsed > 0 else 0
                            logging.info(f"[Thread {thread_id}] Sent {progress_mb:.1f}/{total_mb:.1f} MB ({speed:.1f} MB/s)")
                    
                except socket.error as e:
                    logging.error(f"[Thread {thread_id}] Send error: {e}")
                    raise
            
            send_time = time.time() - start_time
            logging.info(f"[Thread {thread_id}] Command sent successfully in {send_time:.2f}s")
            
            # Signal end of transmission (important for server to know when to stop reading)
            try:
                sock.shutdown(socket.SHUT_WR)
            except:
                pass
            
            # Receive response
            response_data = b""
            sock.settimeout(600)  # 2 minutes timeout for response
            
            start_time = time.time()
            
            while True:
                try:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    
                    response_data += chunk
                    
                    # Check for response terminator
                    if b"\r\n\r\n" in response_data:
                        break
                            
                except socket.timeout:
                    if len(response_data) > 0:
                        logging.info(f"[Thread {thread_id}] Response timeout, but have data ({len(response_data)} bytes)")
                        break
                    else:
                        raise socket.timeout("No response received within timeout")
            
            receive_time = time.time() - start_time
            logging.info(f"[Thread {thread_id}] Response received in {receive_time:.2f}s ({len(response_data)} bytes)")
            
            # Process response
            try:
                clean_data = response_data.decode('utf-8', errors='ignore').replace("\r\n\r\n", "").strip()
                if clean_data:
                    result = json.loads(clean_data)
                    status = result.get('status', 'UNKNOWN')
                    logging.info(f"[Thread {thread_id}] Command completed with status: {status}")
                    return result
                else:
                    raise ValueError("Empty response from server")
                        
            except json.JSONDecodeError as e:
                logging.error(f"[Thread {thread_id}] JSON decode error: {e}")
                logging.error(f"[Thread {thread_id}] Raw response (first 500 chars): {clean_data[:500]}")
                return {"status": "ERROR", "data": f"Invalid JSON response: {str(e)}"}
            
        except socket.timeout:
            logging.warning(f"[Thread {thread_id}] Attempt {attempt + 1} timed out")
            if attempt < max_retries - 1:
                wait_time = min(2 ** attempt, 10)  # Cap backoff at 10 seconds
                logging.info(f"[Thread {thread_id}] Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            continue
            
        except ConnectionRefusedError:
            logging.error(f"[Thread {thread_id}] Connection refused - server not running?")
            if attempt < max_retries - 1:
                time.sleep(5)
            continue
            
        except Exception as e:
            logging.error(f"[Thread {thread_id}] Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                wait_time = min(2 ** attempt, 10)
                time.sleep(wait_time)
            continue
        finally:
            if sock:
                try:
                    sock.close()
                except:
                    pass
    
    # All attempts failed
    logging.error(f"[Thread {thread_id}] All {max_retries} attempts failed")
    return {"status": "ERROR", "data": f"All {max_retries} connection attempts failed"}

def remote_list():
    command_str = "LIST"
    hasil = send_command(command_str)
    if hasil and hasil.get('status') == 'OK':
        print("\nDaftar file di server:")
        files = hasil.get('data', [])
        if files:
            for nmfile in files:
                print(f"- {nmfile}")
        else:
            print("Tidak ada file di server")
        return True
    else:
        error_msg = hasil.get('data', 'Unknown error') if hasil else 'No response'
        print(f"Gagal mengambil daftar file: {error_msg}")
        return False

def remote_get(filename=""):
    if not filename.strip():
        print("Nama file tidak boleh kosong")
        return False
        
    command_str = f"GET {filename}"
    hasil = send_command(command_str)
    if hasil and hasil.get('status') == 'OK':
        try:
            namafile = hasil.get('data_namafile')
            isifile_b64 = hasil.get('data_file')
            
            if not namafile or not isifile_b64:
                print("Response tidak lengkap dari server")
                return False
                
            isifile = base64.b64decode(isifile_b64)
            with open(namafile, 'wb') as fp:
                fp.write(isifile)
            print(f"File '{namafile}' berhasil diunduh ({len(isifile)} bytes).")
            return True
        except Exception as e:
            print(f"Error saving file: {e}")
            return False
    else:
        error_msg = hasil.get('data', 'Unknown error') if hasil else 'No response'
        print(f"Gagal mengunduh file '{filename}': {error_msg}")
        return False

def remote_upload(filename=""):
    if not filename.strip():
        print("Nama file tidak boleh kosong")
        return False
        
    if not os.path.isfile(filename):
        print(f"File {filename} tidak ditemukan")
        return False

    try:
        # Check file size
        file_size = os.path.getsize(filename)
        thread_id = threading.current_thread().ident
        
        # Warn for very large files
        if file_size > 100 * 1024 * 1024:  # 100MB
            response = input(f"File berukuran {file_size / 1024 / 1024:.1f} MB. Lanjutkan? (y/n): ")
            if response.lower() != 'y':
                print("Upload dibatalkan")
                return False
        
        logging.info(f"[Thread {thread_id}] Uploading file {filename} ({file_size / 1024 / 1024:.1f} MB)")
        
        # Read and encode file
        start_time = time.time()
        with open(filename, 'rb') as f:
            filedata = base64.b64encode(f.read()).decode()
        
        encode_time = time.time() - start_time
        logging.info(f"[Thread {thread_id}] File encoded in {encode_time:.2f}s")
        
        basename = os.path.basename(filename)
        
        # Construct command - space-separated format that matches server parsing
        command_str = f"UPLOAD {basename} {filedata}"
        
        # Send with robust retry logic
        hasil = send_command(command_str, max_retries=3)
        
        if hasil and hasil.get('status') == 'OK':
            total_time = time.time() - start_time
            speed = file_size / total_time / 1024 / 1024 if total_time > 0 else 0
            logging.info(f"[Thread {thread_id}] Upload completed in {total_time:.2f}s ({speed:.1f} MB/s)")
            print(f"File '{basename}' berhasil diupload.")
            return True
        else:
            error_msg = hasil.get('data', 'Unknown error') if hasil else 'No response'
            logging.error(f"[Thread {thread_id}] Upload failed: {error_msg}")
            print(f"Gagal mengupload file '{filename}': {error_msg}")
            return False
            
    except Exception as e:
        logging.error(f"[Thread {thread_id}] Error during upload: {e}")
        print(f"Error during upload: {e}")
        return False
    
def main():
    while True:
        print("\n=== CLIENT FILE SYSTEM ===")
        print("1. List file di server")
        print("2. Upload file ke server")
        print("3. Download file dari server")
        print("4. Keluar")
        choice = input("Pilih menu [1-4]: ")

        if choice == '1':
            remote_list()
        elif choice == '2':
            fname = input("Masukkan nama file yang ingin diupload: ")
            remote_upload(fname)
        elif choice == '3':
            fname = input("Masukkan nama file yang ingin diunduh: ")
            remote_get(fname)
        elif choice == '4':
            print("Keluar dari program.")
            break
        else:
            print("Pilihan tidak valid, coba lagi.")

if __name__ == '__main__':
    main()
