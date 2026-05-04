import os

from .sdf_file_processing import preprocess_sdf

def sdf_preprocessing_phase(current_database_dir):
    
    for file in os.listdir(current_database_dir):
        input_file_path = os.path.join(current_database_dir, file)

        if os.path.isdir(input_file_path) and not os.path.islink(input_file_path):
            sdf_preprocessing_phase(input_file_path)

        elif file.lower().endswith('.sdf'):
            try:
                preprocess_sdf(input_file_path)
            except Exception as e:
                print(f"Error processing {input_file_path}: {e}")
