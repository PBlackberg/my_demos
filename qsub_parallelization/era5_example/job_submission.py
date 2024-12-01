'''
# -----------------
#  job_submission
# -----------------
Submits job to node

To delete jobs:
qstat | grep username | awk '{print $1}' | xargs qdel
# ex: 
qstat | grep cb4968 | awk '{print $1}' | xargs qdel  

To query jobs:
qstat -u username
# ex: 
qstat -u cb4968

'''


# -- Packages --
import os
import sys
import numpy as np

# -- Imported scripts --
sys.path.insert(0, os.getcwd())
import utils.util_qsub.submission_funcs as sJ
import utils.util_qsub.my_settings      as mS


# -- give to node --
if __name__ == '__main__':
    # -- specify user settings
    username, SU_project, data_projects, scratch_project, folder_scratch = mS.get_user_specs()
    # [print(f) for f in [username, folder_scratch, SU_project, data_projects]]
    # exit()

    # -- specify resources -- 
    walltime = '0:05:00'    # hh:mm:ss
    mem = '3GB'
    ncpus = 1

    # -- specify variables to give to node -- 
    env_variables = {}  # make key capital letters to distinguish from normal variables
    env_variables["DATASET"] = 'era5'   
    env_variables["VAR_NAME"] = 'ttr'   # OLR
    env_variables["TIMESCALE"] = 'daily'
    years_range = ['1999-1999']
    year1 = years_range[0].split('-')[0]
    year2 = years_range[0].split('-')[1]
    years = range(int(year1), int(year2)+1)

    # -- specify calc_script -- 
    python_script = f'{os.getcwd()}/{env_variables["DATASET"]}_example/section_calc.py'

    print(f'''
-- Running qsub {env_variables["DATASET"]}_example --
dataset:    {env_variables["DATASET"]}
variable:   {env_variables["VAR_NAME"]}
timescale:  {env_variables["TIMESCALE"]}
''')

    # -- submit script -- 
    path_gen = f'/g/data/rt52/era5/single-levels/reanalysis/{env_variables["VAR_NAME"]}'
    folders_year = [f for f in os.listdir(path_gen) if (f.isdigit() and int(f) in years)] # making sure requested years are available as a folder
    folders_year = sorted(folders_year, key=int)    
    # print(folders_year)
    # exit()
    for year in folders_year:
        env_variables["YEAR"] = str(year)
        env_variables["PATH_FOLDER"] = f'/g/data/rt52/era5/single-levels/reanalysis/{env_variables["VAR_NAME"]}/{year}'
        files_data_month = [f for f in os.listdir(env_variables["PATH_FOLDER"]) if f.endswith('.nc')]
        files_data_month = sorted(files_data_month, key=lambda x: x[x.index("sfc_")+1:x.index("-")])
        # print(env_variables["PATH_FOLDER"])
        # print(env_variables["YEAR"])
        # exit()
        for month, file in enumerate(files_data_month):
            if month >= 2:
                break
            env_variables["FILE"] = file
            env_variables["MONTH"] = str(month)
            # print(env_variables["FILE"])
            # print(env_variables["MONTH"])
            sJ.submit_job(python_script = python_script,
                        folder = f'{folder_scratch}/oe_files/qsub_parallelization',                             # oe_folder                            
                        filename = f'{env_variables["DATASET"]}_example_year_{str(year)}_month_{str(month)}',   # oe_filename
                        env_variables = env_variables,
                        walltime = walltime, 
                        mem = mem, 
                        ncpus = ncpus,
                        SU_project = SU_project, 
                        data_projects = data_projects,
                        scratch_project = scratch_project
                        )
    sJ.check_qstat(username, delay = 2) 



