import os
import sys
import time
import tempfile
import functools
import concurrent.futures
import multiprocessing as mp

from .mpds_cl.mpds_cl_all_output_generater import generate_mpds_cl_output

def time_it(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        print(f"Execution time for {func.__name__}: {duration/60:.2f} minutes")
        return result
    return wrapper

def input_file_generator(filepath):
    try:
        with open(filepath, 'r',encoding = 'utf-8') as f:
            for line in f:
                stripped = line.strip()
                if stripped:
                    yield stripped
    except Exception as e:
        print(f"Error reading input file {filepath}: {e}")
        sys.exit(1)

def worker_generate_mpds_all_output(input_queue, mpds_output_path):
    try:
        with open(mpds_output_path, 'w') as mpds_output_f:
            while True:
                stripped_line = input_queue.get()
                if stripped_line is None:  # Sentinel to stop
                    break
                items = stripped_line.strip().split('\t')
                if len(items) < 3:
                    continue
                
                smiles, inchikey, db_ids = items[0], items[1], items[2]
                result = generate_mpds_cl_output(smiles)
                
                if result is None:
                    pass
                else:
                    result['mol_wt'] = f"{float(result['mol_wt']):.2f}" 
                    output_line = f"{smiles}\t{inchikey}\t{db_ids}\t{result['class_id']}\t{result['fp_final']}\t{result['mol_wt']}\n"
                    mpds_output_f.write(output_line)

    except Exception as e:
        print(f"Error in worker process: {e}")

@time_it
def process_input_streaming(input_file, temp_dir, num_workers=4):
    ctx = mp.get_context("fork" if sys.platform != "win32" else "spawn")
    queues = [ctx.Queue(maxsize=1000) for _ in range(num_workers)]

    temp_mpds_output_files = [os.path.join(temp_dir, f'mpds_output_{i+1}.txt') for i in range(num_workers)]

    workers = []
    for i in range(num_workers):
        p = ctx.Process(target=worker_generate_mpds_all_output, args=( queues[i], temp_mpds_output_files[i]))
        p.start()
        workers.append(p)

    generator = input_file_generator(input_file)
    for idx, line in enumerate(generator):
        queues[idx % num_workers].put(line)

    for q in queues:
        q.put(None)

    for p in workers:
        p.join()

    return temp_mpds_output_files

def merge_temp_outputs(temp_files, final_output_path):
    total_lines = 0
    try:
        with open(final_output_path, 'w') as outfile:
            for temp_file in temp_files:
                with open(temp_file, 'r') as infile:
                    for line in infile:
                        outfile.write(line)
                        total_lines += 1
    except Exception as e:
        print(f"Error while merging temp files: {e}")
        sys.exit(1)
    return total_lines


def start_generating_mpds_output(input_file, mpds_output_file, num_workers=4):

    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory: {temp_dir}")

        print("Processing input stream in parallel...")
        temp_mpds_output_files = process_input_streaming(input_file, temp_dir, num_workers)

        print("Merging MPDS output temp files...")
        total_new = merge_temp_outputs(temp_mpds_output_files, mpds_output_file)

        print(f"\n✅ Done!")
        print(f"📦 MPDS output written to: {mpds_output_file} ({total_new} entries)")

def generate_mpds_fp_and_class_with_multiprocessing(input_file, mpds_output_file , num_workers = 4):
    # """
    # To be used after run_cl_pipeline
    # """
    start_generating_mpds_output(input_file, mpds_output_file, num_workers)
