
import multiprocessing as mp

from mpds_cl_and_pipeline.cl_pipeline_phase_1_to_5 import run_phase_1_to_5
from mpds_cl_and_pipeline.cl_pipeline_6_find_new_and_redundant_inchikeys import find_new_and_redundant_inchikeys
from mpds_cl_and_pipeline.cl_pipeline_7_generate_mpds_cl_fp_and_class_id \
    import generate_mpds_fp_and_class_with_multiprocessing

from mpds_cl_and_pipeline.cl_pipeline_8_split_on_class_id_sort_on_mol_wt import split_on_class_id_sort_on_mol_wt
from mpds_cl_and_pipeline.cl_pipeline_9_generate_cluster_files import generate_cluster_files


# ---------------------------------------------------------- Phase 1 to 5 ---------------------------------------------------------- #
databases_dir = '/home/acdsd3/Desktop/2025 March Compound Library Databases/Downloaded Databases'
merged_databases_file_path = 'Merged_databases.txt'

# extracted_databases_dir = 'Extracted_Databases_dir'

mapped_inchikey_id_smiles_file_path = run_phase_1_to_5(
    databases_dir = databases_dir,
    merged_databases_file_path = merged_databases_file_path
    # parse_only_db = "3 Mcule Database",
    # merge_databases = False,
)
# ---------------------------------------------------------------------------------------------------------------------------------- #

# ---------------------------------------------------------- Phase 6 --------------------------------------------------------------- #
### Phase 6
num_workers = max(1,int(mp.cpu_count()/2))  # Set the number of workers

ref_dir = '/home/acdsd3/Desktop/2025 March Compound Library Databases/2024 CL/CSV_VERSION' # path_to_compound_library_csv_version
new_output_file = 'new_inchikey_file.txt' # sys.argv[3]
redundant_output_file = 'redundant_inchikey_file.txt' # sys.argv[4]

find_new_and_redundant_inchikeys(
    mapped_inchikey_id_smiles_file_path,
    ref_dir, 
    new_output_file, 
    redundant_output_file, 
    num_workers
    )

# ---------------------------------------------------------------------------------------------------------------------------------- #

# ---------------------------------------------------------- Phase 7 --------------------------------------------------------------- #
### Phase 7
## To use MPDS-CL with the pipeline
mpds_output_file = 'MPDS_output.txt'
generate_mpds_fp_and_class_with_multiprocessing(
    new_output_file, 
    mpds_output_file, 
    num_workers
    )
# output order -> smiles - inchikey - db_ids - class_id - fp_final - mol_wt
# ---------------------------------------------------------------------------------------------------------------------------------- #

# ---------------------------------------------------------- Phase 8 --------------------------------------------------------------- #

# Splits into classes sorted by molecular weight
class_wise_sorted_dir = 'class_wise_sorted_dir'

split_on_class_id_sort_on_mol_wt(
    mpds_output_file, 
    class_wise_sorted_dir, 
    num_workers
    )
# ---------------------------------------------------------------------------------------------------------------------------------- #

# ---------------------------------------------------------- Phase 9 --------------------------------------------------------------- #
cluster_dir = 'class_files_cluster_wise'
generate_cluster_files(
    class_wise_sorted_dir,
    cluster_dir
    )

# ---------------------------------------------------------------------------------------------------------------------------------- #
