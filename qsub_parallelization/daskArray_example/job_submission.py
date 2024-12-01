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
    walltime = '0:10:00'    # hh:mm:ss
    mem = '60GB'
    ncpus = 1

    # -- specify variables to give to node -- 
    env_variables = {}  # make key capital letters to distinguish from normal variables
    env_variables["DATASET"] = 'daskArray'   
    env_variables["VAR_NAME"] = 'daskArray'
    env_variables["TIMESCALE"] = 'daily'
    years_range = ['1998-2000']
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
    for year in years:
        env_variables["YEAR"] = str(year)
        sJ.submit_job(python_script = python_script,
                      folder = f'{folder_scratch}/oe_files/qsub_parallelization',   # oe_folder                            
                      filename = f'{env_variables["DATASET"]}_example_{str(year)}', # oe_filename
                      env_variables = env_variables,
                      walltime = walltime, 
                      mem = mem, 
                      ncpus = ncpus,
                      SU_project = SU_project, 
                      data_projects = data_projects,
                      scratch_project = scratch_project
                      )
    sJ.check_qstat(username, delay = 2) 



