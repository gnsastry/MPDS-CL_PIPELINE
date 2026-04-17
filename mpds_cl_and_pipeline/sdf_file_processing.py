import os
import re
import pandas as pd
from rdkit import Chem

## 6.1 Gets Fields or Columns Present in SDF
def fetch_sdf_fields(filepath):
    # fields
    unique_fields = set()  # To store all unique fields
    
    with open(filepath, 'r',encoding='utf-8') as file:
        in_data_field = False
        field_name = None

        for i,line in enumerate(file):
            line = line.rstrip()

            # Start of a new molecule
            if line == "$$$$":
                in_data_field = False
                field_name = None
                continue
            # ------------------------------------------------------------------------------------------------------------ # 
            # Start of a data field
            #if line.startswith("> <"):
                #field_name = line[3:-1]  # Extract field name
                #unique_fields.add(field_name)  # Add the field to the set of unique fields
                #in_data_field = True
                #continue
            # ------------------------------------------------------------------------------------------------------------ #
            if re.match(r"^>\s*<", line):  # Match '>' followed by any number of spaces and then '<'
                field_name = line[line.find('<') + 1:-1]  # Extract field name
                unique_fields.add(field_name)  # Add the field to the set of unique fields
                in_data_field = True
                continue

    unique_fields = list({ ' '.join(field.strip().split()) for field in unique_fields })
    unique_fields.sort()
    return unique_fields

## 6.2 Converts SDF to TXT

def parse_sdf_with_fields(input_sdf_file, fields, output_dir):
    """
    Parses an SDF file and extracts all fields (tags and values) for each molecule.
    Also keeps track of all unique fields encountered.
    Args:
        input_sdf_file (str): Path to the SDF file.
    """
    
    sdf_filename_ext = os.path.basename( input_sdf_file )
    filename = sdf_filename_ext.split('.')[0]
    
    output_filename = f'parsed_{filename}.txt'
    output_path = os.path.join(output_dir, output_filename)
    
    with open( input_sdf_file, 'r',encoding='utf-8') as file:
        molecule = {}
        in_data_field = False
        field_name = None
        idx = 0
        for line in file:
            line = line.rstrip()

            # Start of a new molecule
            if line == "$$$$":
                if molecule:
                    # print(f'Fetched :{idx} compounds',end='\r')
                    for key, value in molecule.items():
                        molecule[key] = ' '.join(value.split())
                        
                    if idx == 0:
                        df = pd.DataFrame(columns = fields)
                        df.loc[0] = molecule
                        df.to_csv(output_path, sep='\t',index=False)
                    else:
                        df = pd.DataFrame(columns = fields)
                        df.loc[0] = molecule
                        df.to_csv(output_path, sep='\t', index = False,header=False, mode='a')
                    idx += 1
                    molecule = {}
                    
                in_data_field = False
                field_name = None
                continue

            # Start of a data field
            # ------------------------------------------------------------------------------------------------------------ #
            # if line.startswith("> <"):
                #field_name = line[3:-1]  # Extract field name
                #molecule[field_name] = ""  # Initialize field with empty value
                #in_data_field = True
                #continue
            # ------------------------------------------------------------------------------------------------------------ #
            
            if re.match(r"^>\s*<", line):
                field_name = line[line.find('<') + 1:-1]
                molecule[field_name] = ""  # Initialize field with empty value
                in_data_field = True
                continue

            # Read data for the current field
            if in_data_field:
                if field_name:
                    molecule[field_name] += line + "\n"  # Append data to the field
                continue

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
