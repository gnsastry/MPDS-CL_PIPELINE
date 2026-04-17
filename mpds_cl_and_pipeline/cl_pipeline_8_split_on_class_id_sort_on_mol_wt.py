import os
import sys
from concurrent.futures import ThreadPoolExecutor  # or ProcessPoolExecutor

def process_value(val, input_file, class_wise_sorted_dir,class_id_idx):
    output_path = os.path.join(class_wise_sorted_dir, f"{val}.txt")
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:
        for line in infile:
            if not line.strip():
                continue
            columns = line.rstrip('\n').split('\t')
            if len(columns) < 6:
                continue
            if columns[class_id_idx] == str(val):
                outfile.write(line)

def sort_file_by_column(file_path, col_index):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = [line.rstrip('\n') for line in f if line.strip()]
    
    lines.sort(key=lambda line: line.split('\t')[col_index])
    
    with open(file_path, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')

def split_on_class_id_sort_on_mol_wt(input_file, class_wise_sorted_dir, num_threads = 4 ):
    class_id_col_idx = 3
    mol_wt_col_idx = 5
    
    # class_wise_sorted_dir = 'class_wise_sorted_compounds'
    os.makedirs(class_wise_sorted_dir, exist_ok=True)
    
    # num_threads = max(1,int(os.cpu_count()/2)) 

    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' does not exist.")
        sys.exit(1)
    
    values = [ f'0{i}' if i <10 else str(i) for i in range(1, 57)  ]  # 1 to 56 inclusive

    # Phase 1: Split by 4th column value using threads
    print("Splitting file by  4th column...")
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for val in values:
            executor.submit(process_value, val, input_file, class_wise_sorted_dir, class_id_col_idx)

    # Phase 2: Sort each file by 6th column
    print("Sorting each output file by (Molecular weight) 6th column...")
    
    print("Sorting output files...")
    for val in values:
        file_path = os.path.join(class_wise_sorted_dir, f"{val}.txt")
        if os.path.exists(file_path):
            sort_file_by_column(file_path, mol_wt_col_idx)

    return class_wise_sorted_dir
