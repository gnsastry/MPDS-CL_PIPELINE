import os
import re
import pandas as pd

## 6.1 Gets Fields or Columns Present in SDF
def fetch_sdf_fields(filepath):
    unique_fields = set()
    
    pattern = re.compile(r"^>\s*<([^>]+)>")

    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()

            if line == "$$$$":
                continue

            match = pattern.match(line)
            if match:
                field_name = match.group(1)
                unique_fields.add(field_name)

    # Normalize whitespace and sort
    unique_fields = sorted(
        {' '.join(field.strip().split()) for field in unique_fields}
    )

    return unique_fields

## 6.2 Converts SDF to TXT

def parse_sdf_with_fields(input_sdf_file, fields, output_dir):

    pattern = re.compile(r"^>\s*<([^>]+)>")

    filename = os.path.splitext(os.path.basename(input_sdf_file))[0]
    output_path = os.path.join(output_dir, f'parsed_{filename}.txt')

    rows = []
    molecule = {}
    field_name = None

    with open(input_sdf_file, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.rstrip()

            if line == "$$$$":
                if molecule:
                    # Normalize whitespace
                    for key in molecule:
                        # molecule[key] = ' '.join(molecule[key].split())
                        molecule[key] = ' '.join(molecule[key])
                        molecule[key] = ' '.join(molecule[key].split())

                    rows.append(molecule)

                molecule = {}
                field_name = None
                continue

            match = pattern.match(line)
            if match:
                field_name = match.group(1)
                molecule[field_name] = []
                continue

            if field_name:
                if line == "":  # End of field
                    field_name = None
                else:
                    molecule[field_name].append(line)

    # Handle last molecule if no $$$$
    if molecule:
        for key in molecule:
            molecule[key] = ' '.join(molecule[key]).strip()
        rows.append(molecule)

    # Convert to DataFrame once
    df = pd.DataFrame(rows)

    # Ensure all expected fields exist
    for col in fields:
        if col not in df.columns:
            df[col] = None

    df = df[fields]  # reorder columns

    df.to_csv(output_path, sep='\t', index=False)

def convert_sdf_to_txt(input_sdf_file,output_dir):
    fields = fetch_sdf_fields(input_sdf_file)
    parse_sdf_with_fields(input_sdf_file,fields,output_dir)


## 6.3 Pre - Process SDF File

def preprocess_sdf( input_sdf_file ):
    """
    Replaces ' ' with '_' in SDF File
    """
    sdf_fields = fetch_sdf_fields( input_sdf_file )

    # sdf_fields_with_spaces
    
    field_has_space = True if any( len(field.strip().split()) > 1 for field in sdf_fields ) else False
    
    if field_has_space:
        sdf_file_dir = os.path.dirname( input_sdf_file )
        sdf_file_name_with_ext = os.path.basename( input_sdf_file )
        
        output_filename_with_ext = 'processed_' + sdf_file_name_with_ext
        output_file_path = os.path.join( sdf_file_dir, output_filename_with_ext ) 
        
        input_sdf_file_object  = open( input_sdf_file, 'r', encoding='utf-8' )
        output_sdf_file_object = open( output_file_path, 'w', encoding='utf-8' )
                                     
        for line in input_sdf_file_object:
            processed_line = line
            for field in sdf_fields:
                new_field = field.replace( ' ', '_' )
                processed_line = processed_line.replace( field, new_field )
            output_sdf_file_object.write(processed_line)
                
        input_sdf_file_object.close()
        output_sdf_file_object.close()
        os.remove(input_sdf_file)
