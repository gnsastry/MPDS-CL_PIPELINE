import os
from rdkit import Chem

def generate_chemical_data_from_sdf(db_id_header, input_dir, rdkit_generated_data_dir):
    if db_id_header is None:
        print('---> Attention')
        print("---> Database ID Not Found !!!")
        return  # Exit early

    if len(db_id_header) > 1:
        print('---> Attention')
        print('---> More than 1 Database ID Found')
        return  # Exit early

    db_id_col = db_id_header[0]
    input_dir_files = os.listdir(input_dir)  # Corrected from os.list()
    
    # unique_inchikeys = set()
    
    for file in input_dir_files:
        input_file_path = os.path.join(input_dir, file)
        
        # Check if it's a directory first to avoid processing directories as files
        if os.path.isdir(input_file_path):
            # Recursive call for subdirectories
            generate_chemical_data_from_sdf(db_id_header, input_file_path, rdkit_generated_data_dir)
            continue  # Skip to the next file after recursion

        # Only process .sdf files
        if input_file_path.endswith('.sdf'):
            file_name = os.path.splitext(file)[0]  # Safer way to get file name
            output_file_path = os.path.join(rdkit_generated_data_dir, f"{file_name}.txt")

            # Read the SDF file using RDKit
            supplier = Chem.SDMolSupplier(input_file_path)

            # Open the output file in append mode
            with open(output_file_path, 'a', encoding='utf-8') as output_file_handler:
                for mol in supplier:
                    if mol is None:  # Skip if RDKit fails to parse the molecule
                        continue
                    
                    try:
                        # Generate Canonical SMILES
                        smiles = Chem.MolToSmiles(mol, canonical=True)

                        # Generate InChIKey
                        inchikey = Chem.MolToInchiKey(mol)

                        # Extract Molecule ID if available
                        mol_id = mol.GetProp(db_id_col) if mol.HasProp(db_id_col) else None
                        mol_id = '_'.join(mol_id.split())
                        
                        # Write only if all values are present
                        if mol_id.strip() and inchikey.strip() and smiles.strip():
                            items = [ mol_id, inchikey, smiles ]
                            output_line = '\t'.join(items) + '\n'
                            output_file_handler.write(output_line)
                    
                    except Exception as e:
                        print(f"⚠️ Error processing molecule: {e}")
