from socket import *
import socket
import multiprocessing
import logging
import concurrent.futures
import time
import gc
import sys
import os
import pickle
from file_protocol import FileProtocol

# Configure logging for multiprocessing
def setup_logging():
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('server_mp.log'),
            logging.StreamHandler()
        ]
    )

def process_client_task(client_data):
    """Process client data in separate process - simplified version"""
    try:
        # Setup logging in child process
        setup_logging()
        
        # Unpack client data
        raw_data, address = client_data
        client_id = f"{address[0]}:{address[1]}"
        
        logging.info(f"[{client_id}] Process {os.getpid()} processing data")
        
        if not raw_data:
            logging.warning(f"[{client_id}] No data received")
            return {"status": "ERROR", "data": "No data received"}
        
        data_mb = len(raw_data) / 1024 / 1024
        logging.info(f"[{client_id}] Processing {data_mb:.1f} MB of data")
        
        # Decode with error handling
        try:
            command_str = raw_data.decode('utf-8', errors='replace').strip()
        except UnicodeDecodeError as e:
            logging.error(f"[{client_id}] Unicode decode failed: {e}")
            return {"status": "ERROR", "data": "Failed to decode command data"}
        
        # Log command type safely
        if len(command_str) > 100:
            cmd_preview = command_str[:50] + f"... [TRUNCATED - {len(command_str)} total chars]"
        else:
            cmd_preview = command_str
        logging.info(f"[{client_id}] Processing command: {cmd_preview}")
        
        # Create FileProtocol instance (fresh in each process)
        fp = FileProtocol()
        
        # Process with timing
        start_time = time.time()
        try:
            hasil = fp.proses_string(command_str)
        except Exception as e:
            logging.error(f"[{client_id}] Command processing failed: {e}")
            hasil = '{"status": "ERROR", "data": "Command processing failed"}'
        
        process_time = time.time() - start_time
        logging.info(f"[{client_id}] Command processed in {process_time:.2f}s")
        
        # Clean up memory
        del raw_data
        del command_str
        gc.collect()
        
        return {"status": "SUCCESS", "data": hasil}
        
    except Exception as e:
        logging.error(f"Process task error: {e}")
        return {"status": "ERROR", "data": f"Processing error: {str(e)}"}

def receive_data_from_client(connection, address):
    """Receive all data from client with robust handling"""
    client_id = f"{address[0]}:{address[1]}"
    
    try:
        # Set socket options
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8388608)  # 8MB
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8388608)  # 8MB
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        connection.settimeout(600)  # 10 minutes timeout
        
        full_data = b""
        chunk_size = 2097152  # 2MB chunks
        max_data_size = 10000 * 1024 * 1024  # 10GB max
        last_activity = time.time()
        no_data_timeout = 120
        
        logging.info(f"[{client_id}] Starting to receive data")
        
        while len(full_data) < max_data_size:
            try:
                connection.settimeout(120)  # 2 minutes per chunk
                chunk = connection.recv(chunk_size)
                
                if not chunk:
                    logging.info(f"[{client_id}] Client closed connection, received {len(full_data)} bytes total")
                    break
                
                full_data += chunk
                last_activity = time.time()
                
                # Progress logging every 10MB
                if len(full_data) % (10 * 1024 * 1024) == 0:
                    mb_received = len(full_data) / 1024 / 1024
                    logging.info(f"[{client_id}] Received {mb_received:.1f} MB")
                    
                    # Garbage collection every 50MB
                    if mb_received % 50 == 0:
                        gc.collect()
                
                # Check for small commands
                if len(full_data) < 1024:
                    try:
                        data_str = full_data.decode('utf-8', errors='ignore')
                        if '\n' in data_str or data_str.strip().upper() in ['LIST']:
                            logging.info(f"[{client_id}] Small command detected")
                            break
                    except:
                        pass
                
                # Timeout check
                if time.time() - last_activity > no_data_timeout:
                    logging.info(f"[{client_id}] No data timeout, processing {len(full_data)} bytes")
                    break
                    
            except socket.timeout:
                elapsed = time.time() - last_activity
                if len(full_data) > 0 and elapsed > 30:
                    logging.info(f"[{client_id}] Timeout, processing {len(full_data)} bytes")
                    break
                elif len(full_data) == 0 and elapsed > 60:
                    logging.warning(f"[{client_id}] No data received within timeout")
                    return None
                else:
                    continue
                    
            except (ConnectionResetError, ConnectionAbortedError):
                logging.warning(f"[{client_id}] Connection reset")
                if len(full_data) > 0:
                    break
                return None
                
            except Exception as e:
                logging.error(f"[{client_id}] Error receiving data: {e}")
                if len(full_data) > 0:
                    break
                return None
        
        if len(full_data) == 0:
            return None
            
        data_mb = len(full_data) / 1024 / 1024
        logging.info(f"[{client_id}] Total data received: {data_mb:.1f} MB")
        
        return full_data
        
    except Exception as e:
        logging.error(f"[{client_id}] Exception in receive_data: {e}")
        return None

def send_response_to_client(connection, response_data, address):
    """Send response back to client"""
    client_id = f"{address[0]}:{address[1]}"
    
    try:
        response = response_data + "\r\n\r\n"
        response_bytes = response.encode('utf-8')
        
        bytes_sent = 0
        send_chunk_size = 131072  # 128KB chunks
        connection.settimeout(300)  # 5 minutes for sending
        
        while bytes_sent < len(response_bytes):
            try:
                chunk_end = min(bytes_sent + send_chunk_size, len(response_bytes))
                chunk = response_bytes[bytes_sent:chunk_end]
                sent = connection.send(chunk)
                
                if sent == 0:
                    raise ConnectionError("Socket connection broken")
                
                bytes_sent += sent
                
                # Progress for large responses
                if bytes_sent % (1024 * 1024) == 0 and len(response_bytes) > 1024 * 1024:
                    logging.info(f"[{client_id}] Sent {bytes_sent / 1024 / 1024:.1f} MB")
                    
            except Exception as e:
                logging.error(f"[{client_id}] Error sending response: {e}")
                return False
        
        logging.info(f"[{client_id}] Response sent successfully ({len(response_bytes)} bytes)")
        return True
        
    except Exception as e:
        logging.error(f"[{client_id}] Exception in send_response: {e}")
        return False

class Server:
    def __init__(self, ipaddress='0.0.0.0', port=6666, max_workers=4):
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        
        # Simple counters (no locks needed in main process)
        self.total_connections = 0
        self.active_connections = 0
        
        # Create socket
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Try SO_REUSEPORT (Linux/macOS)
        try:
            self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        
        # Socket buffer sizes
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8388608)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8388608)
        
        # Create process pool
        self.executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=max_workers,
            mp_context=multiprocessing.get_context('spawn')
        )
        
        logging.info(f"Server initialized with {max_workers} worker processes")
    
    def handle_client(self, connection, address):
        """Handle client in main process, then delegate processing"""
        client_id = f"{address[0]}:{address[1]}"
        
        try:
            # Receive data in main process
            raw_data = receive_data_from_client(connection, address)
            
            if raw_data is None:
                logging.warning(f"[{client_id}] No data received")
                return
            
            # Submit processing task to worker process
            future = self.executor.submit(process_client_task, (raw_data, address))
            
            try:
                # Wait for result with timeout
                result = future.result(timeout=1200)  # 20 minutes max
                
                if result["status"] == "SUCCESS":
                    # Send successful response
                    success = send_response_to_client(connection, result["data"], address)
                    if success:
                        logging.info(f"[{client_id}] Request completed successfully")
                    else:
                        logging.error(f"[{client_id}] Failed to send response")
                else:
                    # Send error response
                    error_response = f'{{"status": "ERROR", "data": "{result["data"]}"}}'
                    send_response_to_client(connection, error_response, address)
                    logging.error(f"[{client_id}] Request failed: {result['data']}")
                    
            except concurrent.futures.TimeoutError:
                logging.error(f"[{client_id}] Processing timeout")
                error_response = '{"status": "ERROR", "data": "Processing timeout"}'
                send_response_to_client(connection, error_response, address)
                
            except Exception as e:
                logging.error(f"[{client_id}] Processing exception: {e}")
                error_response = f'{{"status": "ERROR", "data": "Processing failed: {str(e)}"}}'
                send_response_to_client(connection, error_response, address)
                
        except Exception as e:
            logging.error(f"[{client_id}] Exception in handle_client: {e}")
        finally:
            try:
                connection.close()
                logging.info(f"[{client_id}] Connection closed")
            except:
                pass
            
            self.active_connections -= 1
            logging.info(f"Active connections: {self.active_connections}")
    
    def run(self):
        logging.info(f"Server starting on {self.ipinfo}")
        
        try:
            self.my_socket.bind(self.ipinfo)
            self.my_socket.listen(100)
            
            logging.info("Server ready to accept connections")
            
            while True:
                try:
                    self.my_socket.settimeout(1.0)
                    
                    try:
                        conn, client_address = self.my_socket.accept()
                    except socket.timeout:
                        continue
                    
                    # Check capacity
                    if self.active_connections >= self.max_workers:
                        logging.warning(f"Server at capacity ({self.active_connections} connections)")
                        try:
                            error_msg = '{"status": "ERROR", "data": "Server busy, try again later"}\r\n\r\n'
                            conn.settimeout(5)
                            conn.send(error_msg.encode())
                            conn.close()
                        except:
                            pass
                        continue
                    
                    self.active_connections += 1
                    self.total_connections += 1
                    
                    logging.info(f"New connection from {client_address} (Active: {self.active_connections})")
                    
                    # Handle client in separate thread (not process) to avoid serialization issues
                    import threading
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(conn, client_address),
                        daemon=True
                    )
                    client_thread.start()
                    
                except KeyboardInterrupt:
                    logging.info("Shutdown signal received")
                    break
                except Exception as e:
                    logging.error(f"Error accepting connection: {e}")
                    time.sleep(0.1)
                    
        except Exception as e:
            logging.error(f"Server error: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        logging.info("Shutting down server...")
        try:
            self.executor.shutdown(wait=True, timeout=60)
            self.my_socket.close()
            logging.info(f"Final stats - Total connections served: {self.total_connections}")
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")
        
        logging.info("Server shutdown complete")

def main():
    # Setup logging in main process
    setup_logging()
    
    # Set multiprocessing start method
    if hasattr(multiprocessing, 'set_start_method'):
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass
    
    # Create and run server
    svr = Server(ipaddress='0.0.0.0', port=6666, max_workers=4)
    try:
        svr.run()
    except KeyboardInterrupt:
        logging.info("Server interrupted by user")
    except Exception as e:
        logging.error(f"Server crashed: {e}")

if __name__ == "__main__":
    main()