from collections import defaultdict

def Map_InChIKey_to_ID_SMILES(input_file,output_file):
    
    # Dictionary to store {unique_col1_value: (set(col2_values), set(col3_values))}
    data_dict = defaultdict(lambda: (set(), set()))
    
    # Read large file line by line
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            row = line.split('\t')
            

            if len(row) >= 3:  # Ensure at least 3 columns exist
    
                db_id = row[0].strip()
                inchikey = row[1].strip()
                smiles = row[2].strip()
    
                data_dict[inchikey][0].add(db_id)  # Store unique col2 values
                data_dict[inchikey][1].add(smiles)  # Store unique col3 values
            else:
                print('len(row) >= 3 :',len(row) >= 3)
                
    # Write to output file
    with open(output_file, "w", encoding="utf-8") as f:
        for inchikey, (db_id, smiles) in data_dict.items():
            items = [
                list(sorted(smiles))[0],
                inchikey,
                ",".join(sorted(db_id))
            ]
            line = '\t'.join(items) + '\n'
            f.write(line)
