from socket import *
import socket
import threading
import logging
import concurrent.futures
import time
import gc
import sys
from file_protocol import FileProtocol

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler()
    ]
)

def process_client(connection, address):
    """Process client connection with robust error handling and memory management"""
    client_id = f"{address[0]}:{address[1]}"
    logging.info(f"[{client_id}] Client connected")
    
    try:
        # Set socket options for large data transfer
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8388608)  # 8MB receive buffer
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8388608)  # 8MB send buffer
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)     # Disable Nagle's algorithm
        
        # Set longer timeout for large file operations
        connection.settimeout(600)  # 10 minutes timeout
        
        # Enhanced data receiving with memory monitoring
        full_data = b""
        chunk_size = 2097152  # 2MB chunks (larger for better performance)
        max_data_size = 10000 * 1024 * 1024  # 6000MB max
        last_activity = time.time()
        no_data_timeout = 120  # Increased timeout
        
        logging.info(f"[{client_id}] Starting to receive data")
        
        # Track memory usage
        initial_memory = gc.get_count()
        
        while len(full_data) < max_data_size:
            try:
                connection.settimeout(120)  # 1 minute per chunk
                chunk = connection.recv(chunk_size)
                
                if not chunk:
                    logging.info(f"[{client_id}] Client closed connection, received {len(full_data)} bytes total")
                    break
                
                full_data += chunk
                last_activity = time.time()
                
                # Enhanced progress logging
                if len(full_data) % (10 * 1024 * 1024) == 0:  # Every 10MB
                    mb_received = len(full_data) / 1024 / 1024
                    logging.info(f"[{client_id}] Received {mb_received:.1f} MB")
                    
                    # Force garbage collection every 50MB to prevent memory buildup
                    if mb_received % 50 == 0:
                        gc.collect()
                        logging.debug(f"[{client_id}] Garbage collection triggered at {mb_received:.1f} MB")
                
                # Check for command completion patterns
                if len(full_data) < 1024:
                    try:
                        data_str = full_data.decode('utf-8', errors='ignore')
                        if '\n' in data_str or data_str.strip().upper() in ['LIST']:
                            logging.info(f"[{client_id}] Small command detected, processing")
                            break
                    except:
                        pass
                
                # Enhanced timeout handling
                if time.time() - last_activity > no_data_timeout:
                    logging.info(f"[{client_id}] No data for {no_data_timeout}s, assuming transmission complete")
                    break
                    
            except socket.timeout:
                elapsed_since_data = time.time() - last_activity
                if len(full_data) > 0 and elapsed_since_data > 30:
                    logging.info(f"[{client_id}] Timeout after {elapsed_since_data:.1f}s, processing {len(full_data)} bytes")
                    break
                elif len(full_data) == 0 and elapsed_since_data > 60:
                    logging.warning(f"[{client_id}] No data received within extended timeout")
                    return
                else:
                    continue
                    
            except ConnectionResetError:
                logging.warning(f"[{client_id}] Connection reset by client")
                if len(full_data) > 0:
                    logging.info(f"[{client_id}] Processing {len(full_data)} bytes received before reset")
                    break
                return
                
            except Exception as e:
                logging.error(f"[{client_id}] Error receiving data: {e}")
                if len(full_data) > 0:
                    logging.info(f"[{client_id}] Processing {len(full_data)} bytes received before error")
                    break
                return
        
        if len(full_data) == 0:
            logging.warning(f"[{client_id}] No data received")
            return
            
        data_mb = len(full_data) / 1024 / 1024
        logging.info(f"[{client_id}] Total data received: {data_mb:.1f} MB")
        
        # Memory check before processing
        if sys.getsizeof(full_data) > 200 * 1024 * 1024:  # 200MB
            logging.warning(f"[{client_id}] Large data size: {sys.getsizeof(full_data) / 1024 / 1024:.1f} MB")
        
        # Process the command with error handling
        try:
            # Decode with better error handling
            try:
                command_str = full_data.decode('utf-8', errors='replace').strip()
            except UnicodeDecodeError as e:
                logging.error(f"[{client_id}] Unicode decode failed: {e}")
                raise ValueError("Failed to decode command data")
            
            # Log command type safely
            if len(command_str) > 100:
                cmd_preview = command_str[:50] + f"... [TRUNCATED - {len(command_str)} total chars]"
            else:
                cmd_preview = command_str
            logging.info(f"[{client_id}] Processing command: {cmd_preview}")
            
            # Create FileProtocol instance
            fp = FileProtocol()
            
            # Process with timeout monitoring
            start_time = time.time()
            try:
                hasil = fp.proses_string(command_str)
            except Exception as e:
                logging.error(f"[{client_id}] Command processing failed: {e}")
                hasil = '{"status": "ERROR", "data": "Command processing failed"}'
            
            process_time = time.time() - start_time
            logging.info(f"[{client_id}] Command processed in {process_time:.2f}s")
            
            # Clear the large data from memory immediately
            del full_data
            del command_str
            gc.collect()
            
            # Send response with chunking
            response = hasil + "\r\n\r\n"
            response_bytes = response.encode('utf-8')
            
            # Enhanced response sending
            bytes_sent = 0
            send_chunk_size = 131072  # 128KB chunks for response
            connection.settimeout(120)  # 5 minutes for sending response
            
            while bytes_sent < len(response_bytes):
                try:
                    chunk_end = min(bytes_sent + send_chunk_size, len(response_bytes))
                    chunk = response_bytes[bytes_sent:chunk_end]
                    sent = connection.send(chunk)
                    
                    if sent == 0:
                        raise ConnectionError("Socket connection broken during send")
                    
                    bytes_sent += sent
                    
                    # Progress logging for large responses
                    if bytes_sent % (1024 * 1024) == 0 and len(response_bytes) > 1024 * 1024:
                        logging.info(f"[{client_id}] Sent {bytes_sent / 1024 / 1024:.1f} MB")
                        
                except Exception as e:
                    logging.error(f"[{client_id}] Error sending response: {e}")
                    break
            
            if bytes_sent == len(response_bytes):
                logging.info(f"[{client_id}] Response sent successfully ({len(response_bytes)} bytes)")
            else:
                logging.warning(f"[{client_id}] Partial response sent ({bytes_sent}/{len(response_bytes)} bytes)")
            
        except Exception as e:
            logging.error(f"[{client_id}] Error processing command: {e}")
            error_response = f'{{"status": "ERROR", "data": "Processing error: {str(e)}"}}\r\n\r\n'
            try:
                connection.send(error_response.encode())
            except:
                pass
                
    except Exception as e:
        logging.error(f"[{client_id}] Exception in client handling: {e}")
    finally:
        try:
            connection.close()
            logging.info(f"[{client_id}] Connection closed")
        except:
            pass
        
        # Aggressive garbage collection and memory cleanup
        gc.collect()
        logging.debug(f"[{client_id}] Memory cleanup completed")


class Server:
    def __init__(self, ipaddress='0.0.0.0', port=6666, max_workers=10):  # Reduced max workers
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.active_connections = 0
        self.connection_lock = threading.Lock()
        self.stats = {'total_connections': 0, 'failed_connections': 0}
        
        # Create socket with enhanced options
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Increase socket buffer sizes
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8388608)  # 8MB
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8388608)  # 8MB
        
        # Create thread pool with conservative settings
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="ClientHandler"
        )
        
        logging.info(f"Server initialized with max {max_workers} workers")
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        self.monitor_thread.start()
    
    def _monitor_resources(self):
        """Monitor server resources"""
        while True:
            try:
                with self.connection_lock:
                    active = self.active_connections
                
                # Log status every 30 seconds
                logging.info(f"Server status - Active connections: {active}, Total served: {self.stats['total_connections']}")
                
                # Force garbage collection if memory usage is high
                if active > self.max_workers * 0.8:
                    gc.collect()
                    logging.warning("High connection load - triggered garbage collection")
                
                time.sleep(30)
            except Exception as e:
                logging.error(f"Monitor thread error: {e}")
                time.sleep(60)
    
    def handle_client_wrapper(self, conn, client_address):
        """Enhanced wrapper to track active connections"""
        with self.connection_lock:
            self.active_connections += 1
            self.stats['total_connections'] += 1
            logging.info(f"Active connections: {self.active_connections}")
        
        try:
            process_client(conn, client_address)
        except Exception as e:
            logging.error(f"Client handler error: {e}")
            with self.connection_lock:
                self.stats['failed_connections'] += 1
        finally:
            with self.connection_lock:
                self.active_connections -= 1
                logging.info(f"Active connections: {self.active_connections}")
    
    def run(self):
        logging.info(f"Server starting on {self.ipinfo}")
        
        try:
            self.my_socket.bind(self.ipinfo)
            self.my_socket.listen(100)  # Increased backlog
            
            logging.info("Server ready to accept connections")
            
            while True:
                try:
                    # Accept connection with timeout
                    self.my_socket.settimeout(1.0)  # 1 second timeout for accept
                    
                    try:
                        conn, client_address = self.my_socket.accept()
                    except socket.timeout:
                        continue  # Check for shutdown conditions
                    
                    # Check capacity with better handling
                    with self.connection_lock:
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
                    
                    logging.info(f"New connection from {client_address}")
                    
                    # Submit to thread pool with better error handling
                    try:
                        self.executor.submit(self.handle_client_wrapper, conn, client_address)
                    except Exception as e:
                        logging.error(f"Failed to submit client to thread pool: {e}")
                        try:
                            conn.close()
                        except:
                            pass
                    
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
        """Enhanced cleanup"""
        logging.info("Shutting down server...")
        try:
            # Shutdown thread pool gracefully
            self.executor.shutdown(wait=True, timeout=60)
            self.my_socket.close()
            
            # Final stats
            logging.info(f"Final stats - Total connections: {self.stats['total_connections']}, Failed: {self.stats['failed_connections']}")
            
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")
        
        logging.info("Server shutdown complete")

def main():
    # Set conservative worker count for large file handling
    svr = Server(ipaddress='0.0.0.0', port=6666, max_workers=10)
    try:
        svr.run()
    except KeyboardInterrupt:
        logging.info("Server interrupted by user")
    except Exception as e:
        logging.error(f"Server crashed: {e}")

if __name__ == "__main__":
    main()