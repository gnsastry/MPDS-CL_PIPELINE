import os
import gzip
import shutil
import zipfile

# ---------------------------------------- file decompressor ------------------------------------------ #

def extract_file(file_path, extract_dir):
    """
    Extracts the contents of a ZIP or GZ file to the specified directory.
    """
    
    if file_path.endswith(".zip"):
        # Handle ZIP files
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
        except Exception as e:
            filename = os.path.basename( file_path )
            print(f'Error Decompressing --> {filename}')

    elif file_path.endswith(".gz"):
        # Handle GZ files
        base_name = os.path.basename(file_path).replace('.gz', '')
        output_path = os.path.join(extract_dir, base_name)
        try:
            with gzip.open(file_path, 'rb') as gz_ref:
                with open(output_path, 'wb') as out_file:
                    shutil.copyfileobj(gz_ref, out_file)
        except Exception as e:
            filename = os.path.basename( file_path )
            print(f'Error Decompressing --> {filename}')        
        

def decompression_phase( current_database_dir, decompressed_dir ):
    database = os.path.basename( current_database_dir )

    # log = create_logger( current_database_dir, 'Decompression' )
    # compressed_found = False
    
    current_database_dir_files = os.listdir( current_database_dir )
    
    for file in current_database_dir_files:
        if file.endswith('.gz') or file.endswith('.zip'):
            # compressed_found = True
            input_file_path = os.path.join(current_database_dir,file)
            
            try :
                extract_file( input_file_path, decompressed_dir)
            except Exception as e:
                # log.error(f"{database} {file} Error While Decompressing !!! {e}")
                print(f"{database} {file} Error While Decompressing !!! {e}")

# ----------------------------------------------------------------------------------------------------- #