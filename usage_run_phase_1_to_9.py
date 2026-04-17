import os

from mpds_cl_and_pipeline.cl_pipeline_phase_1_to_9 import run_phase_1_to_9

num_workers = max(1,int(os.cpu_count()/2))

databases_dir = '/run/media/acdsd2/0133-804F/In-House-MPDS-CL-and-Pipeline/DATABASES'
ref_dir = '/run/media/acdsd2/0133-804F/In-House-MPDS-CL-and-Pipeline/ref_dir'

run_phase_1_to_9(
    databases_dir = databases_dir,
    ref_dir = ref_dir,
    num_workers = num_workers #,
    # phase_1=False,
    # phase_2=False,
    # phase_3=False
    # phase_4=False
    # parse_only_db='4 SureChEMBL'
)




