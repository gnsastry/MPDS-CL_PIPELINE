# Standard Library
import os

import warnings
# Suppress all Python warnings
warnings.filterwarnings("ignore")

# Third party Library
from rdkit import RDLogger

# Suppress RDKit warnings
RDLogger.DisableLog('rdApp.*')

from .cl_pipeline_utilities import dir_contains_sdf
from .cl_pipeline_1_decompression import decompression_phase

# SDF FILE
from .cl_pipeline_2_sdf_1_preprocessing import sdf_preprocessing_phase
from .cl_pipeline_2_sdf_2_sdf_to_txt_conversion_phase import sdf_to_txt_conversion_phase
from .cl_pipeline_2_sdf_3_find_id_for_sdf_txt import find_id_from_sdf_txt_files
from .cl_pipeline_2_sdf_4_generate_chemical_data_from_sdf import generate_chemical_data_from_sdf

# TABULAR FILE
from .cl_pipeline_2_tabular_1_identify_tabular_data_file_format_and_headers import identify_file_ext_sep_and_headers
from .cl_pipeline_2_tabular_2_generate_data_from_smiles import generate_chemical_data_from_smiles

# MERGING ALL DATABASES
from .cl_pipeline_3_merge_respective_database_files import remove_duplicate_inchikey

# InChIKey to SMILES MAPPING
from .cl_pipeline_4_merge_downloaded_databases import merge_all_databases
from .cl_pipeline_5_InChIKey_to_ID_SMILES_Mapper import Map_InChIKey_to_ID_SMILES

from typing import List

def run_phase_1_to_5(
        databases_dir:str,
        merged_databases_file_path:str,
        unqiue_compound_merged_database_file_path:str,

        # for Debugging
        parse_only_db=False,
        # merge_databases= True,

        ## Default Arguments
        # Directory Names
        decompressed_dir_name = 'decompressed_dir',
        sdf_txt_dir_name = 'sdf_txt_dir',
        rdkit_generated_data_dir_name = 'rdkit_generated_data_dir',
        final_processed_database_dir_name = 'final_processed_database_dir',
        not_processed_dir_name = 'not_processed_dir',
        extracted_databases_dir = 'Extracted_Databases_dir',

        skip_db:List[str] = [],

        phase_1=True,
        phase_2=True,
        phase_3=True,
        phase_4=True,
        phase_5=True
    ):
    """
    Input:
        databases_dir              : Path to directory containing databases each inside its own folder
        merged_databases_file_path : Combined databases file_path
        parse_only_db              : To extract only specified database
        skip_db                    : List of Database folder names to Skip Processing 

    Description:
        - Extracts Compound Data Database_ID, InChIKey and SMILES. 
        - Removes Redundancy based on InChIKey and Stores into a file
        - Saves into tab-separated txt file 

    Output:
        - Saves unique Molecules with a suffix '_extracted_databases_InChIkey_to_ID_SMILES_Mapping.txt'
        - Order of output : SMILES  InChIKey    Database_ID

    Returns:
        - Path to Extracted unique molecules
    """


    # 4. Databases List
    databases = os.listdir( databases_dir )

    # 7. Databases to be SKIPPED
    # skip_db = [ 'Therapeutic Target Database', 'Lipid', '9 NCI Cactus', '15 Oprea. DrugCentral', 'COD'  ]


    # 8. Phase 1 - 3
    for idx, database in enumerate( databases, 1):
        if parse_only_db:
            if parse_only_db not in database:
                continue
        
        if any(text in database for text in skip_db): # 
            continue
        
        # if not any(val in database for val in tabular_databases):
        #     continue

        n = 65
        print('-'*n, database, '-'*n)
        
        current_database_dir = os.path.join( databases_dir, database )
        output_database_dir = os.path.join(extracted_databases_dir, database)

        # -------------------------------------- Output Directories ------------------------------------------------- #
        # -------------------> decompressed_dir

        # decompressed_dir = os.path.join( current_database_dir, decompressed_dir_name)
        decompressed_dir = os.path.join( current_database_dir, decompressed_dir_name)

        os.makedirs( decompressed_dir, exist_ok = True)
        
        # -------------------> sdf_txt_dir
        # sdf_txt_dir = os.path.join( current_database_dir, sdf_txt_dir_name)
        sdf_txt_dir = os.path.join( output_database_dir, sdf_txt_dir_name)

        os.makedirs( sdf_txt_dir, exist_ok = True)

        # -------------------> rdkit_generated_data_dir
        # rdkit_generated_data_dir = os.path.join( current_database_dir, rdkit_generated_data_dir_name )
        rdkit_generated_data_dir = os.path.join( output_database_dir, rdkit_generated_data_dir_name )

        os.makedirs( rdkit_generated_data_dir, exist_ok = True )

        # -------------------> final_processed_database_dir
        # final_processed_database_dir = os.path.join( current_database_dir, final_processed_database_dir_name )
        final_processed_database_dir = os.path.join( output_database_dir, final_processed_database_dir_name )

        os.makedirs( final_processed_database_dir, exist_ok = True )

        # -------------------> not_processed_dir
        # not_processed_dir = os.path.join( current_database_dir, not_processed_dir_name )
        not_processed_dir = os.path.join( output_database_dir, not_processed_dir_name )

        os.makedirs( not_processed_dir, exist_ok = True )
        # -------------------------------------------------------------------------------------------------------------- #

        # -------------------------------------------------------------------------------------------------------------- #
        # Phase 1
        if phase_1:
            print('Phase 1 : Decompression Phase')
            decompression_phase( current_database_dir, decompressed_dir)
            
            try:
                os.rmdir(decompressed_dir)
            except:
                pass

        # -------------------------------------- Checking For SDF File ------------------------------------------------- #
        sdf_file_found = dir_contains_sdf(current_database_dir)
                                        
        if not sdf_file_found:
            try:
                os.rmdir(sdf_txt_dir)
            except:
                pass
        # -------------------------------------------------------------------------------------------------------------- #
        if phase_2:
            if sdf_file_found :
                # Phase 2 : SDF Step 1 
                print('Phase 2 : SDF File processing')
                print('-> Step 1 : SDF Preprocessing Phase')
                
                sdf_preprocessing_phase(current_database_dir)
                # -------------------------------------------------------------------------------------------------------------- #
            
                # -------------------------------------------------------------------------------------------------------------- #
                # Phase 2 : SDF Step 2
                print('Phase 2 : SDF File processing')
                print('-> Step 2 : SDF To TXT Conversion Phase')
                
                sdf_to_txt_conversion_phase( current_database_dir, sdf_txt_dir)
                
                # Phase 2 : SDF Step 3
                # print('Phase 2 : SDF File processing')
                print('---> Step 3 : Find Database ID Header from SDF TXT')        
                
                db_id_header = find_id_from_sdf_txt_files( sdf_txt_dir )
                print(db_id_header)
                print()
                
                # Phase 2 : SDF Step 4
                # print('Phase 2 : SDF File processing')
                print('---> Step 4 : Generate Chemical Data ( InChIKey, SMILES ) From SDF Using RDkit')
                generate_chemical_data_from_sdf(db_id_header, current_database_dir, rdkit_generated_data_dir)
                print()
            else:
                # Phase 2 : Tabular Step 1
                print('Phase 2 : Tabular File processing')
                print('---> Step 1 : Identify file extension, separator and headers')
                ext_sep_headers = identify_file_ext_sep_and_headers(current_database_dir)
                print(ext_sep_headers)
                
                # Phase 2 : Tabular Step 2
                # print('Phase 2 : Tabular File processing')
                print('---> Step 2 : Generate Chemical Data ( Canonical SMILES, InChIKey ) from SMILES')
                generate_chemical_data_from_smiles( current_database_dir, rdkit_generated_data_dir, not_processed_dir, ext_sep_headers )
                print()

        if phase_3:
            # Phase 3 : Merge Database
            print('Phase 3 : Remove Duplicate InChIKeys')
            remove_duplicate_inchikey( rdkit_generated_data_dir, final_processed_database_dir)
            print()
        
        try:
            os.rmdir(rdkit_generated_data_dir)
        except:
            pass

    if phase_4:
        # 9. Phase 4
        print('Phase 4 : Merge All Databases')

        # merged_databases_file_path = '2025_Downloaded_Databases_cl_pipeline_2.txt'

        print(f'merged_databases_file_path : {merged_databases_file_path}')
        # skip_db = [ 'Therapeutic Target Database', 'Lipid', '9 NCI Cactus', '15 Oprea. DrugCentral', 'COD'  ]
        
        merge_all_databases( 
            databases_dir = databases_dir, 
            extracted_databases_dir = extracted_databases_dir,
            final_processed_database_dir_name = final_processed_database_dir_name, 
            merged_databases_file_path = merged_databases_file_path, 
            skip_db = skip_db
            )

    if phase_5:
        # 10. Phase 5
        print('Phase 5 : Map InChIKey to Database Id and SMILES ( Unique Compounds ) ')
        print(f'unqiue_compound_merged_database_file_path : {unqiue_compound_merged_database_file_path}')

        Map_InChIKey_to_ID_SMILES( input_file = merged_databases_file_path, output_file = unqiue_compound_merged_database_file_path )

        return unqiue_compound_merged_database_file_path
    
    return None