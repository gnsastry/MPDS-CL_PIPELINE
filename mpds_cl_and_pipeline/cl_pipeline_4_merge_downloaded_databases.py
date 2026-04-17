import os
from tqdm import tqdm

def merge_all_databases( databases_dir, extracted_databases_dir, final_processed_database_dir_name, merged_databases_file_path, skip_db = [] ):
    databases = os.listdir( databases_dir )
    
    file_paths = []
    
    for database in databases:
        if any(val in database for val in skip_db):
            continue  # Skip unwanted databases
        
        current_database_dir = os.path.join(extracted_databases_dir, database)
        # print(current_database_dir)    

        final_processed_database_dir = os.path.join(current_database_dir, final_processed_database_dir_name)
        # print(final_processed_database_dir)

        if os.path.exists(final_processed_database_dir):
            final_processed_database_dir_files = os.listdir( final_processed_database_dir )
            # print(f"{database} : {len(final_processed_database_dir_files)}")
    
            for file in final_processed_database_dir_files:
                file_path = os.path.join(final_processed_database_dir, file)
                file_paths.append(file_path)
    
    print('All Database File Paths Fetched')
    print(f'Total Files : {len(file_paths)}')

    # Iterate over file_paths with tqdm for progress tracking
    for file_path in tqdm( file_paths, desc="Processing Files", unit="file", colour="green"):
        
        with open(file_path, 'r', encoding='utf-8') as input_file, \
            open(merged_databases_file_path, 'a', encoding='utf-8') as output_file:
                try:
                    for line in input_file:
                        output_file.write(line)
                except Exception as e:
                    print(file_path)