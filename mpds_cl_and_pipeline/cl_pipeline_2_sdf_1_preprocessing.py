import os

from .sdf_file_processing import preprocess_sdf

def sdf_preprocessing_phase( current_database_dir ) :
    
    current_database_dir_files = os.listdir( current_database_dir )
    
    for file in current_database_dir_files:
        
        input_file_path = os.path.join( current_database_dir, file )
        
        if file.endswith('.sdf'):
            preprocess_sdf( input_file_path )
            
        elif os.path.isdir( input_file_path ):
            sdf_preprocessing_phase( input_file_path )
            
        else:
            pass