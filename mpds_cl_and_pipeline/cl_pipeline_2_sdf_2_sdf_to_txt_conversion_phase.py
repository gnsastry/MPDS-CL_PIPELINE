import os
from .sdf_file_processing import convert_sdf_to_txt

# To process database and convert sdf files

def sdf_to_txt_conversion_phase( current_database_dir, sdf_to_txt_dir):
    
    files = os.listdir( current_database_dir )
    
    for file in files:
        input_file_path = os.path.join(current_database_dir,file)
        if input_file_path.endswith('.sdf'):
            convert_sdf_to_txt( input_file_path, sdf_to_txt_dir)
            
        elif os.path.isdir(input_file_path):
            sdf_to_txt_conversion_phase(input_file_path, sdf_to_txt_dir)
        elif input_file_path.endswith('.sdf'):
            # print(file)
            convert_sdf_to_txt( input_file_path, sdf_to_txt_dir)
        else:
            pass

    