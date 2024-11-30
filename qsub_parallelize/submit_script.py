'''
# -----------------
#  job_submission
# -----------------
To delete jobs:
qstat | grep cb4968 | awk '{print $1}' | xargs qdel
qstat -u cb4968

'''


import numpy as np
import util_parallel.submission_funcs as sJ

import os
import sys
sys.path.insert(0, os.getcwd())
import utils.util_files.save_folders    as sF
import utils.util_core.choose_datasets  as cD  



if __name__ == '__main__':
    # -- specify python script -- 
    walltime = '0:05:00'
    mem = '130GB'
    python_script = f'{os.getcwd()}/dask_demo/calc_script.py'


    # -- specify variables to give to script -- 
    switch_var = {'rlut': True}
    experiment = 'obs'
    dataset = 'ERA5'
    var_name = next((key for key, value in switch_var.items() if value), None)
    resolution = 'regridded'
    timescale = 'daily'
    years_range = ['1998-2000'] # cD.obs_years
    source = 'obs'
    print(f'dataset: {dataset}')
    print(f'variable: {var_name}')
    year1 = years_range[0].split('-')[0]
    year2 = years_range[0].split('-')[1]
    years = range(int(year1), int(year2)+1)
    var_nameERA = sJ.convert_to_era_var_name(var_name)
    era_type = 'reanalysis' 
    path_gen = f'/g/data/rt52/era5/single-levels/{era_type}/{var_nameERA}'
    folders_year = [f for f in os.listdir(path_gen) if (f.isdigit() and int(f) in years)]
    folders_year = sorted(folders_year, key=int)


    # -- submit script with specified resources -- 
    for year in folders_year:
        path_folder = os.path.join(path_gen, year)
        pbs_script = sJ.create_resource_script(python_script = python_script,
                                            folder = f'{sF.folder_scratch}/oe_files/data_processed/{var_name}/{source}/{timescale}/{dataset}', 
                                            filename = f'{dataset}_{var_name}_{year}', 
                                            walltime = walltime, 
                                            mem = mem
                                            )
        sJ.submit_job(python_script, pbs_script, path_folder, var_name, var_nameERA, year, dataset)
    sJ.check_qstat('cb4968', delay = 2)


