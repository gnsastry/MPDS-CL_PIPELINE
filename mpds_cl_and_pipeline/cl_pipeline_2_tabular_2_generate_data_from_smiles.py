import os

from .cl_pipeline_utilities import get_items_sorted_by_size

from .find_chemical_data import (
    identify_chemical_columns,
    contains_any_chemical_column,
    get_canonical_smiles_inchikey,
    # is_smiles, 
    remove_metal_hydrogens 
)

from .tabular_file_processing import create_dataframe

def generate_chemical_data_from_smiles(
    input_dir, output_dir, not_processed_dir, ext_sep_headers,
    visited_dirs=None, processed_files=None
):
    if visited_dirs is None:
        visited_dirs = set()
    if processed_files is None:
        processed_files = set()

    real_input_dir = os.path.realpath(input_dir)
    if real_input_dir in visited_dirs:
        return
    visited_dirs.add(real_input_dir)

    file_extension = ext_sep_headers['ext']
    separator = ext_sep_headers['sep']
    db_has_headers = ext_sep_headers['db_has_headers']
    header = 0 if db_has_headers else None

    db_id_cols = ext_sep_headers['ID']
    smiles_cols = ext_sep_headers['SMILES']

    if not db_id_cols:
        print('---> Attention !!! No ID Found')
        return
    elif len(db_id_cols) > 1:
        print('---> Attention !!! Multiple ID columns found:', db_id_cols)
        return

    db_id_col = db_id_cols[0]
    smiles_col = smiles_cols[0]

    sorted_items = get_items_sorted_by_size(input_dir)
    input_dir_files = [item_name for item_name, item_size in sorted_items]

    for file in input_dir_files:
        input_file_path = os.path.join(input_dir, file)

        if os.path.islink(input_file_path):
            continue
        if os.path.isdir(input_file_path):
            # Recurse into subdirectory
            generate_chemical_data_from_smiles(
                input_file_path, output_dir, not_processed_dir,
                ext_sep_headers, visited_dirs, processed_files
            )
            continue

        if not input_file_path.endswith(file_extension):
            continue

        real_file_path = os.path.realpath(input_file_path)
        if real_file_path in processed_files:
            continue  # Already processed

        processed_files.add(real_file_path)

        print(f'Processing file: {file}')
        filename = os.path.splitext(file)[0]
        output_file_path = os.path.join(output_dir, f'{filename}.txt')
        not_processed_smiles_file_path = os.path.join(not_processed_dir, 'SMILES_not_processed.txt')

        df = create_dataframe(input_file_path, ext=file_extension, sep=separator, header=header)
        if df.empty:
            print(f'--> Skipped empty file: {file}')
            continue
        
        df_columns = set(df.columns)
        # print(f'Columns present in {file} : {df_columns}' )
        
        found_chemical_columns = set(db_id_cols + smiles_cols)
        # print('Found Chemical Columns :', found_chemical_columns)

        if found_chemical_columns.issubset(df_columns):
            # print('Chemical data column found in file')
            with open(output_file_path, 'w') as output_file_handler, \
                open(not_processed_smiles_file_path, 'a') as not_processed_file_handler:

                for idx in df.index:
                    db_id_value = ""
                    smiles_value = ""

                    try:
                        db_id_value = str(df.loc[idx, db_id_col]).strip()
                        db_id_value = '_'.join(db_id_value.split())

                        smiles_value = df.loc[idx, smiles_col]
                        smiles_value = smiles_value.strip() if isinstance(smiles_value, str) else None

                        if not smiles_value:
                            continue

                        clean_smiles = remove_metal_hydrogens(smiles_value)
                        canonical_smiles, inchikey = get_canonical_smiles_inchikey(clean_smiles)

                        items = [db_id_value, inchikey, canonical_smiles]
                        output_line = '\t'.join(items) + '\n'
                        output_file_handler.write(output_line)

                    except Exception:
                        output_line = '\t'.join([db_id_value, smiles_value or ""]) + '\n'
                        not_processed_file_handler.write(output_line)
