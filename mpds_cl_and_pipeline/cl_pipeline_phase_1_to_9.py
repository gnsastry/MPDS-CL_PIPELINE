import os
from typing import List

from .cl_pipeline_phase_1_to_5 import run_phase_1_to_5
from .cl_pipeline_phase_6_to_9 import run_phase_6_to_9

def run_phase_1_to_9(
        databases_dir:str,    
        ref_dir:str,

        merged_databases_file_path:str='Merged_databases.txt',

        # For controlled phase execution
        phase_1_to_5 = True,
        phase_6_to_9 = True,

        # for Debugging
        parse_only_db=False,
        # merge_databases= True,

        ## Default Arguments
        # Directory and File Names
        decompressed_dir_name = 'decompressed_dir',
        sdf_txt_dir_name = 'sdf_txt_dir',
        rdkit_generated_data_dir_name = 'rdkit_generated_data_dir',
        final_processed_database_dir_name = 'final_processed_database_dir',
        not_processed_dir_name = 'not_processed_dir',
        extracted_databases_dir = 'Extracted_Databases_dir',

        new_output_file = 'new_inchikey_file.txt',
        redundant_output_file = 'redundant_inchikey_file.txt', 
        mpds_output_file = 'MPDS_output.txt',
        class_wise_sorted_dir = 'class_wise_sorted_dir',
        cluster_dir = 'cluster_wise_dir',
        num_workers = 4,

        skip_db:List[str] = [],
        phase_1 = True,
        phase_2=True,
        phase_3=True,
        phase_4=True,
        phase_5=True
    ):
    unqiue_compound_merged_database_file = f"Unique_compounds_{os.path.basename(merged_databases_file_path)}"
    unqiue_compound_merged_database_file_path = os.path.join(
            os.path.dirname(merged_databases_file_path),
            unqiue_compound_merged_database_file
    )
    if phase_1_to_5:
        unqiue_compound_merged_database_file_path = run_phase_1_to_5(
            databases_dir = databases_dir,
            merged_databases_file_path = merged_databases_file_path,
            unqiue_compound_merged_database_file_path = unqiue_compound_merged_database_file_path,

            parse_only_db = parse_only_db,
            # merge_databases = merge_databases,

            decompressed_dir_name = decompressed_dir_name,
            sdf_txt_dir_name = sdf_txt_dir_name,
            rdkit_generated_data_dir_name = rdkit_generated_data_dir_name,
            final_processed_database_dir_name = final_processed_database_dir_name,
            not_processed_dir_name = not_processed_dir_name,
            extracted_databases_dir = extracted_databases_dir,
            skip_db = skip_db,
            phase_1 = phase_1,
            phase_2=phase_2,
            phase_3=phase_3,
            phase_4=phase_4,
            phase_5=phase_5
        )

    if unqiue_compound_merged_database_file_path is not None and phase_6_to_9:
        run_phase_6_to_9(
            unqiue_compound_merged_database_file_path = unqiue_compound_merged_database_file_path,
            ref_dir = ref_dir,
            new_output_file = new_output_file,
            redundant_output_file = redundant_output_file,
            mpds_output_file = mpds_output_file,
            class_wise_sorted_dir = class_wise_sorted_dir,
            cluster_dir = cluster_dir,
            num_workers = num_workers
        )