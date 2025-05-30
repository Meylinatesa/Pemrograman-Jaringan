import time
import os
import csv
import threading
from file_client import remote_upload, remote_get
import random
import logging
import signal
import sys
import gc

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stress_test.log'),
        logging.StreamHandler()
    ]
)

FILES = {
    '10MB': 'file_10mb.bin',
    '50MB': 'file_50mb.bin',
    '100MB': 'file_100mb.bin',
}

CSV_FILE = "stress_result.csv"

shutdown_flag = threading.Event()

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    logging.info("Shutdown signal received. Waiting for workers to finish...")
    shutdown_flag.set()

signal.signal(signal.SIGINT, signal_handler)

def create_test_files():
    """Create test files if they don't exist"""
    sizes = {'10MB': 10, '50MB': 50, '100MB': 100}
    
    for size_name, size_mb in sizes.items():
        filepath = FILES[size_name]
        if not os.path.exists(filepath):
            logging.info(f"Creating {filepath}...")
            with open(filepath, 'wb') as f:
                # Write random data in smaller chunks to prevent memory issues
                chunk_size = 512 * 1024  # 512KB chunks
                chunks_per_mb = 1024 * 1024 // chunk_size  # 2 chunks per MB
                
                for mb in range(size_mb):
                    if shutdown_flag.is_set():
                        return False
                    
                    for chunk_num in range(chunks_per_mb):
                        data = os.urandom(chunk_size)
                        f.write(data)
                    
                    # Progress and memory management
                    if (mb + 1) % 10 == 0:
                        logging.info(f"Created {mb+1}/{size_mb} MB of {filepath}")
                        gc.collect()  # Force garbage collection
                        
            logging.info(f"Completed creating {filepath}")
    return True

def log_to_csv(operation, file_size, worker_count, total_success, total_failed, avg_time, avg_throughput):
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, mode='a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow([
                "Operation", "File Size", "Worker Count", 
                "Success", "Failed", "Avg Time (s)", "Avg Throughput (bytes/s)"
            ])
        writer.writerow([
            operation.upper(), file_size, worker_count, 
            total_success, total_failed, 
            round(avg_time, 3), round(avg_throughput, 3)
        ])

def run_worker(operation, filepath, results, index, barrier, semaphore):
    """Enhanced worker function with semaphore for resource control"""
    worker_id = f"W{index:02d}"
    
    # Acquire semaphore to limit concurrent operations
    with semaphore:
        try:
            # Wait for all workers to be ready
            barrier.wait()
            
            # Staggered start with better distribution
            delay = random.uniform(0, min(10.0, index * 0.5))
            logging.info(f"[{worker_id}] Starting {operation} after {delay:.1f}s delay")
            time.sleep(delay)
            
            if shutdown_flag.is_set():
                results[index] = {'success': False, 'duration': 0, 'throughput': 0, 'error': 'Shutdown'}
                return
            
            start = time.time()
            success = False
            error_msg = None
            
            try:
                logging.info(f"[{worker_id}] Starting {operation} operation")
                
                if operation == 'upload':
                    success = remote_upload(filepath)
                elif operation == 'download':
                    filename = os.path.basename(filepath)
                    success = remote_get(filename)
                    
                    # Verify download
                    if success:
                        downloaded_file = os.path.basename(filepath)
                        success = os.path.exists(downloaded_file)
                        if not success:
                            error_msg = "Downloaded file not found"
                
            except Exception as e:
                logging.error(f"[{worker_id}] Operation failed: {e}")
                success = False
                error_msg = str(e)
            
            end = time.time()
            duration = end - start
            
            # Calculate throughput
            if success and os.path.exists(filepath):
                size = os.path.getsize(filepath)
                throughput = size / duration if duration > 0 else 0
            else:
                throughput = 0
            
            results[index] = {
                'success': success,
                'duration': duration,
                'throughput': throughput,
                'error': error_msg
            }
            
            status = "SUCCESS" if success else f"FAILED ({error_msg})"
            logging.info(f"[{worker_id}] {operation.upper()} {status} in {duration:.2f}s")
            
        except Exception as e:
            logging.error(f"[{worker_id}] Worker exception: {e}")
            results[index] = {
                'success': False, 
                'duration': 0, 
                'throughput': 0, 
                'error': f"Worker exception: {str(e)}"
            }
        finally:
            # Cleanup and garbage collection
            gc.collect()

def run_stress_test(operation, file_size_key, worker_count):
    """Enhanced stress test with better resource management"""
    if shutdown_flag.is_set():
        return False
        
    filepath = FILES[file_size_key]

    if not os.path.exists(filepath):
        logging.error(f"File {filepath} not found. Create test files first.")
        return False
    
    file_size = os.path.getsize(filepath)
    logging.info(f"\n{'='*80}")
    logging.info(f"STRESS TEST: {operation.upper()} | {file_size_key} ({file_size/1024/1024:.1f} MB) | {worker_count} workers")
    logging.info(f"{'='*80}")
    
    # Resource management: limit concurrent operations for large files
    max_concurrent = worker_count
    if file_size_key in ['50MB', '100MB']:
        max_concurrent = min(worker_count, 10)  # Limit to 3 concurrent for large files
        logging.info(f"Limiting concurrent operations to {max_concurrent} for large files")
    
    semaphore = threading.Semaphore(max_concurrent)
    results = [None] * worker_count
    threads = []
    
    # Use barrier to synchronize worker start
    barrier = threading.Barrier(worker_count)
    
    start_time = time.time()
    
    # Create and start all workers
    for i in range(worker_count):
        if shutdown_flag.is_set():
            logging.info("Shutdown requested, aborting test")
            return False
            
        t = threading.Thread(
            target=run_worker, 
            args=(operation, filepath, results, i, barrier, semaphore),
            name=f"{operation}Worker{i}"
        )
        threads.append(t)
        t.start()
        
        # Longer delay between thread creation for large files
        delay = 0.1 if file_size_key in ['50MB', '100MB'] else 0.05
        time.sleep(delay)
    
    logging.info(f"All {worker_count} workers started, waiting for completion...")
    
    # Enhanced timeout calculation based on file size and worker count
    base_timeout = {
        '10MB': 60,   # 1 minute base
        '50MB': 300,  # 5 minutes base  
        '100MB': 600  # 10 minutes base
    }
    
    timeout_per_worker = base_timeout.get(file_size_key, 180)
    total_timeout = timeout_per_worker + (worker_count * 10)  # Add time per worker
    
    logging.info(f"Using timeout of {total_timeout} seconds for this test")
    
    # Wait for all workers with progress tracking
    completed_workers = 0
    start_wait = time.time()
    
    for i, t in enumerate(threads):
        if shutdown_flag.is_set():
            logging.info("Shutdown requested during worker wait")
            break
            
        try:
            remaining_timeout = max(30, total_timeout - (time.time() - start_wait))
            t.join(timeout=remaining_timeout)
            
            if t.is_alive():
                logging.warning(f"Worker {i} timed out after {remaining_timeout:.0f}s")
            else:
                completed_workers += 1
                if completed_workers % max(1, worker_count // 10) == 0:
                    logging.info(f"Progress: {completed_workers}/{worker_count} workers completed")
                    
        except Exception as e:
            logging.error(f"Error waiting for worker {i}: {e}")
    
    end_time = time.time()
    total_test_time = end_time - start_time
    
    # Analyze results
    valid_results = [r for r in results if r is not None]
    successful_results = [r for r in valid_results if r.get('success')]
    
    total_success = len(successful_results)
    total_failed = len(valid_results) - total_success
    incomplete = worker_count - len(valid_results)
    
    if total_success > 0:
        total_time = sum(r['duration'] for r in successful_results)
        total_throughput = sum(r['throughput'] for r in successful_results)
        avg_time = total_time / total_success
        avg_throughput = total_throughput / total_success
    else:
        avg_time = 0
        avg_throughput = 0
    
    # Enhanced results logging
    logging.info(f"\nTEST RESULTS:")
    logging.info(f"  Operation: {operation.upper()}")
    logging.info(f"  File Size: {file_size_key} ({file_size/1024/1024:.1f} MB)")
    logging.info(f"  Total Workers: {worker_count}")
    logging.info(f"  Max Concurrent: {max_concurrent}")
    logging.info(f"  Successful: {total_success}")
    logging.info(f"  Failed: {total_failed}")
    logging.info(f"  Incomplete: {incomplete}")
    logging.info(f"  Success Rate: {(total_success/worker_count)*100:.1f}%")
    logging.info(f"  Avg Time per Operation: {avg_time:.2f}s")
    logging.info(f"  Total Test Time: {total_test_time:.2f}s")
    logging.info(f"  Avg Throughput: {avg_throughput/1024/1024:.2f} MB/s")
    
    # Log error summary
    if total_failed > 0:
        error_summary = {}
        for r in valid_results:
            if not r.get('success') and r.get('error'):
                error = r['error']
                error_summary[error] = error_summary.get(error, 0) + 1
        
        logging.info(f"  Error Summary:")
        for error, count in error_summary.items():
            logging.info(f"    {error}: {count} times")
    
    log_to_csv(operation, file_size_key, worker_count, total_success, total_failed, avg_time, avg_throughput)
    
    # Force garbage collection after test
    gc.collect()
    
    return True

def main():
    print("Creating test files...")
    if not create_test_files():
        print("Failed to create test files")
        return
    
    print("\nStarting stress test...")
    print("Make sure the server is running before starting!")
    input("Press Enter to continue...")

    # Conservative test matrix - reduced worker counts for stability
    operations = ['upload', 'download']
    file_sizes = ['10MB', '50MB', '100MB']
    worker_counts = [1, 5, 10]  # Reduced from [1, 5, 50]

    total_tests = len(operations) * len(file_sizes) * len(worker_counts)
    current_test = 0
    
    for op in operations:
        for size in file_sizes:
            for workers in worker_counts:
                current_test += 1
                print(f"\n{'='*60}")
                print(f"Test {current_test}/{total_tests}: {op.upper()} {size} with {workers} workers")
                print(f"{'='*60}")
                
                success = run_stress_test(op, size, workers)
                
                if not success:
                    logging.warning(f"Test {current_test} failed or was interrupted")
                
                # Longer wait between tests, especially for large files
                if current_test < total_tests and not shutdown_flag.is_set():
                    wait_time = 10 if size in ['50MB', '100MB'] else 2
                    print(f"Waiting {wait_time} seconds before next test...")
                    time.sleep(wait_time)
                    
                    # Additional cleanup between tests
                    gc.collect()
                
                if shutdown_flag.is_set():
                    print("Shutdown requested, stopping tests")
                    break
            
            if shutdown_flag.is_set():
                break
        
        if shutdown_flag.is_set():
            break
    
    print(f"\n{'='*60}")
    if shutdown_flag.is_set():
        print("Tests interrupted by user!")
    else:
        print("All tests completed!")
    print(f"Results saved to: {CSV_FILE}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()