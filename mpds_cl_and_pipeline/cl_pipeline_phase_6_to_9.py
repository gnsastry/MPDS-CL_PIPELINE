from .cl_pipeline_6_find_new_and_redundant_inchikeys import find_new_and_redundant_inchikeys
from .cl_pipeline_7_generate_mpds_cl_fp_and_class_id \
    import generate_mpds_fp_and_class_with_multiprocessing

from .cl_pipeline_8_split_on_class_id_sort_on_mol_wt import split_on_class_id_sort_on_mol_wt
from .cl_pipeline_9_generate_cluster_files import generate_cluster_files


def run_phase_6_to_9(
    unqiue_compound_merged_database_file_path, 
    ref_dir,
    new_output_file = 'new_inchikey_file.txt',
    redundant_output_file = 'redundant_inchikey_file.txt', 
    mpds_output_file = 'MPDS_output.txt',
    class_wise_sorted_dir = 'class_wise_sorted_dir',
    cluster_dir = 'cluster_wise_dir',
    num_workers = 4
    ):
    
    # Phase 6
    print('Phase 6 : Compare with previous compound library compounds in ref_dir and Find new and redundant compounds')
    find_new_and_redundant_inchikeys(
        unqiue_compound_merged_database_file_path,
        ref_dir, 
        new_output_file, 
        redundant_output_file, 
        num_workers
        )
    
    # Phase 7
    print('Phase 7 : Generate MPDS FP and Class for New Compounds Found')
    # output order -> smiles - inchikey - db_ids - class_id - fp_final - mol_wt
    generate_mpds_fp_and_class_with_multiprocessing( 
        new_output_file, 
        mpds_output_file, 
        num_workers
        )
    
    #  Phase 8 
    print('Phase 8 : Split Compounds Class-Wise and Sort in Ascending Order of Molecular Weight')
    split_on_class_id_sort_on_mol_wt(
        mpds_output_file, 
        class_wise_sorted_dir, 
        num_workers
        )
    
    # Phase 9
    print('Phase 9 : Generate Class-Wise Cluster Files')
    generate_cluster_files(
        class_wise_sorted_dir,
        cluster_dir
        )
    
