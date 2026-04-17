import os
import pandas as pd

from .cl_pipeline_utilities import get_items_sorted_by_size, centered_string

from .find_chemical_data import identify_chemical_columns


def find_id_from_sdf_txt_files( sdf_txt_dir ):
    """
    Input:
        Full Path to Database directory
    
    Purpose:
        Finds ID, InChI, InChIKey and SMILES from SDF TXT Directory Files

    Returns:
        Database ID Header if single ID Header is found else None
        
    """
    db_id_header = []
    
    # Sorting FIles in increasing Size
    sorted_items = get_items_sorted_by_size( sdf_txt_dir )
    sdf_txt_dir_files = [item_name for item_name, item_size in sorted_items]
    
    # Searches for Datbase_ID, InChI, InChIKey and SMILES 
    for i, file in enumerate(sdf_txt_dir_files, 1):
            
        print(f"\nSDF TXT File {i} :", file)
        
        input_file_path = os.path.join(sdf_txt_dir,file)
        
        df = pd.read_csv(input_file_path, sep='\t')
        
        result = identify_chemical_columns( df )
        
        # Checks whether Any ID is Present
        if len( result['ID'] ) > 0 :
            db_id_header = result['ID']
            break
    return db_id_header