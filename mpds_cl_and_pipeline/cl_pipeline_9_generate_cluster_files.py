import os
import sys
import pandas as pd

def generate_cluster_files(
        class_wise_sorted_dir, 
        cluster_dir, #  = 'class_files_cluster_wise', 
        url = 'https://mpds.neist.res.in:8086/static/clusters.html'):
    
    # class_wise_sorted_compounds_dir = '/home/acdsd3/Desktop/2025 March Compound Library Databases/cl_pipeline_version_2/class_wise_sorted_compounds'
    # class_wise_sorted_compounds_dir = sys.argv[1]
    # class_wise_sorted_compounds_dir_files = os.listdir(class_wise_sorted_compounds_dir)
    
    
    tables = pd.read_html(url)
    
    cluster_table = tables[0]
    
    cluster_table = cluster_table[:-1]
    cluster_table = cluster_table.iloc[:,:len(cluster_table.columns)-1]
    
    cluster_size = 250000
    
    os.makedirs(cluster_dir, exist_ok = True)
    
    for idx in cluster_table.index:
        class_num = cluster_table.loc[idx,'Class no.'].split('_')[1]
        class_num = f'0{class_num}' if len(class_num) == 1 else class_num

        for col_idx in range( len(list(cluster_table.columns)) - 1 , 0 , -1 ):
            prev_cluster_num = cluster_table.iloc[idx,col_idx]
            prev_cluster_num = str(prev_cluster_num).strip()
            if prev_cluster_num != '-':
                break
        class_file = f'{class_num}.txt'
        class_file_path = os.path.join(class_wise_sorted_dir, class_file)

        # print(f'prev_cluster_num : {prev_cluster_num}')
        new_cluster_num = str( int(prev_cluster_num) + 1 )
    
        with open(class_file_path) as input_class_file:
            lines_written = 0
            
            for line in input_class_file:
                if lines_written == 0:
                    
                    cluster_file = f'class_{class_num}_cluster_{new_cluster_num}.txt'
                    cluster_file_path = os.path.join(cluster_dir, cluster_file)
                    
                    with open(cluster_file_path, 'w') as output_file:
                        output_file.write(line)
                        lines_written += 1
                        
                elif lines_written < cluster_size:
                    with open(cluster_file_path, 'a') as output_file:
                        output_file.write(line)
                        lines_written += 1
                        
                elif lines_written == cluster_size:
                    lines_written = 0
                    new_cluster_num = str( int(new_cluster_num) + 1 )
    
                else:
                    pass
