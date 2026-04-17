import os

from .find_chemical_data import is_InChIKey, is_smiles

def remove_duplicate_inchikey(rdkit_generated_data_dir, final_processed_database_dir):
    
    rdkit_generated_data_dir_files = os.listdir(rdkit_generated_data_dir)
    
    inchi_keys = set()
    
    # output_file_path = os.path.join(final_processed_database_dir, 'final_data.txt' )
    
    for file in rdkit_generated_data_dir_files:
        input_file_path = os.path.join( rdkit_generated_data_dir, file )
        output_file_path = os.path.join( final_processed_database_dir, file )
        
        with open(input_file_path,'r',encoding = 'utf-8') as input_file_handler, \
            open(output_file_path,'w',encoding = 'utf-8') as output_file_handler:
                
                for idx,line in enumerate(input_file_handler,1):
                    items = line.split('\t')
                    valid_line = True
                    
                    try:
                        if len(items) != 3:
                            raise ValueError("Incorrect number of fields")
                        db_id = items[0].strip()
                        inchi_key = items[1].strip()
                        smiles = items[2].strip()
                    except Exception as e:
                        print( f'File : {file} | Line Noumber : {idx} | Line : { line.strip() } | Error : {e}' )
                        valid_line = False
                        
                    if valid_line:
                        if is_InChIKey(inchi_key) and is_smiles(smiles) and inchi_key not in inchi_keys:
                            inchi_keys.add( inchi_key )
                            output_line = '\t'.join([ db_id, inchi_key, smiles ]) + '\n'
                            output_file_handler.write( output_line )