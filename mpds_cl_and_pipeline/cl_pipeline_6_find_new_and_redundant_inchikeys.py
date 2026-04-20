#!/usr/bin/env python3

import os
import sys
import time
import tempfile
import functools
import concurrent.futures
import multiprocessing as mp
from find_chemical_data import is_InChIKey

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

def load_file_to_set(filepath):
    local_set = set()
    try:
        # added automatic inchikey detection
        inchikey_idx = -1
        with open(filepath, 'r',encoding = 'utf-8') as f:
            for line in f:
                items = line.strip().split('\t')
                for idx, item in enumerate(items):
                    if is_InChIKey(item[idx]):
                        inchikey_idx = idx
                        break
                break
        if inchikey_idx != -1:
            with open(filepath, 'r',encoding = 'utf-8') as f:
                for line in f:
                    items = line.strip().split('\t')
                    inchikey = items[inchikey_idx]
                    local_set.add(inchikey)
        else:
            print('------ No InChIKey Found in Reference File ------')

    except Exception as e:
        print(f"Error reading reference file {filepath}: {e}")
    return local_set

@time_it
def build_reference_set_multithreaded(ref_dir):
    ref_sets = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for filename in os.listdir(ref_dir):
            filepath = os.path.join(ref_dir, filename)
            if os.path.isfile(filepath):
                futures.append(executor.submit(load_file_to_set, filepath))
        for future in concurrent.futures.as_completed(futures):
            ref_sets.append(future.result())
    merged_ref_set = set().union(*ref_sets)
    print(f'Loaded : {len(merged_ref_set)} Existing InChIKeys')
    # print(f"Sample inchikeys: {list(merged_ref_set)[:4]}")
    return merged_ref_set

def worker_check_and_write(ref_set, input_queue, new_compounds_path, redundant_compounds_path):
    try:
        with open(new_compounds_path, 'w') as new_compounds_f, open(redundant_compounds_path, 'w') as redundant_compounds_f:
            while True:
                stripped_line = input_queue.get()
                if stripped_line is None:  # Sentinel to stop
                    break
                items = stripped_line.strip().split('\t')
                if len(items) < 3:
                    continue
                smiles, inchikey, db_ids = items[0], items[1], items[2]
                output_line = f'{smiles}\t{inchikey}\t{db_ids}\n'
                if inchikey in ref_set:
                    redundant_compounds_f.write(output_line)
                else:
                    new_compounds_f.write(output_line)
    except Exception as e:
        print(f"Error in worker process: {e}")

@time_it
def process_input_streaming(input_file, ref_set, temp_dir, num_workers=4):
    ctx = mp.get_context("fork" if sys.platform != "win32" else "spawn")
    queues = [ctx.Queue(maxsize=1000) for _ in range(num_workers)]

    new_compound_files = [os.path.join(temp_dir, f'new_compound_{i+1}.txt') for i in range(num_workers)]
    redundant_compound_files = [os.path.join(temp_dir, f'redundant_compound_{i+1}.txt') for i in range(num_workers)]

    workers = []
    for i in range(num_workers):
        p = ctx.Process(target=worker_check_and_write, args=(ref_set, queues[i], new_compound_files[i], redundant_compound_files[i]))
        p.start()
        workers.append(p)

    generator = input_file_generator(input_file)
    for idx, line in enumerate(generator):
        queues[idx % num_workers].put(line)

    for q in queues:
        q.put(None)

    for p in workers:
        p.join()

    return new_compound_files, redundant_compound_files

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

def find_new_and_redundant_inchikeys(input_file, ref_dir, new_output_file, redundant_output_file, num_workers=4):
    print("Building reference set...")
    ref_set = build_reference_set_multithreaded(ref_dir)

    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory: {temp_dir}")

        print("Processing input stream in parallel...")
        new_compound_files, redundant_compound_files = process_input_streaming(input_file, ref_set, temp_dir, num_workers)

        print("Merging new compound temp files...")
        total_new = merge_temp_outputs(new_compound_files, new_output_file)

        print("Merging redundant compound temp files...")
        total_redundant = merge_temp_outputs(redundant_compound_files, redundant_output_file)

        print(f"\n✅ Done!")
        print(f"📦 Unique new compounds written to: {new_output_file} ({total_new} entries)")
        print(f"🗂️ Redundant (already existing) compounds written to: {redundant_output_file} ({total_redundant} entries)")

# def main():
#     if len(sys.argv) != 3:
#         script_name = os.path.basename(sys.argv[0])
#         print(f"Usage: python3 {script_name} <input_file> <reference_directory>")
#         sys.exit(1)

#     input_file = sys.argv[1]
#     ref_dir = sys.argv[2]
#     new_output_file = 'new_inchikey_file.txt' # sys.argv[3]
#     redundant_output_file = 'redundant_inchikey_file.txt' # sys.argv[4]

#     num_workers = max(int(mp.cpu_count() / 2), 1)

#     find_new_and_redundant_inchikeys(input_file, ref_dir, new_output_file, redundant_output_file, num_workers)

# if __name__ == "__main__":
#     # mp.freeze_support()
#     main()
