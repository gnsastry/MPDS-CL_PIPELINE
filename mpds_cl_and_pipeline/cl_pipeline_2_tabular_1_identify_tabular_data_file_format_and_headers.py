import os
import re
from itertools import product

from .cl_pipeline_utilities import get_items_sorted_by_size

from .tabular_file_processing import (
    fetch_ext_sep,
    create_dataframe
)

from .find_chemical_data import ( 
    identify_chemical_columns,
    is_InChI, 
    is_InChIKey, 
    is_smiles, 
    is_url,
    contains_any_chemical_column
)

def contains_headers(result):
    """
    Input:
        result (dict): Dictionary with keys ['ID', 'InChI', 'InChIKey', 'SMILES', 'URL']
                       Values should be strings or None.
    Description:
        - Checks if the dictionary contains header-like values.
        - Returns True if likely headers, False if likely data.
    Return:
        bool: True if headers, False if not.
    """

    id_cols = result.get('ID', [] )
    id_cols = [ str(val) for val in id_cols]
    
    inchi_cols = result.get('InChI', [] )
    inchi_key_cols = result.get('InChIKey', [] )
    
    smiles_cols = result.get('SMILES', [] )
    url_cols = result.get('URL', [] )

    # Check for SMILES
    for smiles_col in smiles_cols:
        if isinstance(smiles_col, str) and smiles_col.strip() and is_smiles(smiles_col):
            return False # Not a header
            
    # Check for SMILES and ID
    cartesian_product = list(product(id_cols, smiles_cols))
    for id_col, smiles_col in cartesian_product:
        
        if isinstance(smiles_col, str) and smiles_col.strip() and isinstance(id_col, str) and id_col.strip():
            if is_smiles(smiles_col) and re.search(r"\d", id_col):
                return False # Not a header

    # Check for InChI
    for inchi_col in inchi_cols:
        # if isinstance(inchi_col, str) and re.fullmatch(patterns['InChI'], inchi_col):
        if isinstance(inchi_col, str) and inchi_col.strip() and is_InChI(inchi_col):
            return False # Not a header

    # Check for InChIKey
    for inchi_key_col in inchi_key_cols:
        # if isinstance(inchi_key_col, str) and re.fullmatch(patterns['InChIKey'], inchi_key_col):
        if isinstance(inchi_key_col, str) and inchi_key_col.strip() and is_InChIKey( inchi_key_col ):
            return False # Not a header

    # Check for URL
    for url_col in url_cols:
        # Optionally check URL if required (if URL should not look like a header)
        if isinstance(url_col, str) and url_col.strip() and is_url(url_col):
            return False # Not a headers

    return True  # All checks passed; likely a header


def identify_file_ext_sep_and_headers(database_dir):
    sorted_items = get_items_sorted_by_size(database_dir)
    database_dir_files = [item_name for item_name, item_size in sorted_items]

    ext_sep_headers = {
        "ext": None,
        "sep": None,
        "ID": [],
        "SMILES": [],
        "db_has_headers": None
    }

    for file in database_dir_files:
        input_file_path = os.path.join(database_dir, file)

        # Skip certain folders or extensions
        if any(substring in file for substring in ['sdf_txt_dir', 'rdkit_generated_data_dir']):
            continue
        if file.endswith('.sdf'):
            continue

        # Recurse into subdirectories
        if os.path.isdir(input_file_path):
            inner_result = identify_file_ext_sep_and_headers(input_file_path)
            # If valid data found in subdirectory, return it immediately
            if inner_result and inner_result.get("ext") is not None:
                return inner_result
            continue  # Skip further processing of directories

        # Process supported file extensions
        if any(file.lower().endswith(ext) for ext in {".csv", ".xls", ".xlsx", ".smi", ".tsv", ".txt"}):
            print('File:', file)
            ext, sep = fetch_ext_sep(input_file_path)

            print(f'-> File Extension : {ext}')
            print(f'-> Separator : {sep}')

            df = create_dataframe(input_file_path, ext, sep)
            if df.empty:
                print("Empty DataFrame for:", input_file_path)
                continue

            result = identify_chemical_columns(df)
            found_chemical_col = contains_any_chemical_column(result)

            print('found_chemical_col:', found_chemical_col)

            if len(result['SMILES']) > 0:
                # Populate ext_sep_headers
                ext_sep_headers['ext'] = ext
                ext_sep_headers['sep'] = sep
                ext_sep_headers['ID'] = result['ID']
                ext_sep_headers['SMILES'] = result['SMILES']

                db_has_headers = contains_headers(result)
                ext_sep_headers['db_has_headers'] = db_has_headers

                if db_has_headers is False:
                    # Convert to column indices
                    ext_sep_headers['ID'] = [df.columns.get_loc(col) for col in result['ID']]
                    ext_sep_headers['SMILES'] = [df.columns.get_loc(col) for col in result['SMILES']]

                return ext_sep_headers  # ✅ Return as soon as valid result found

    return ext_sep_headers  # Return default if nothing valid is found
