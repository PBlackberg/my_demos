'''
# ---------------------
#  submission_funcs
# ---------------------
Functions to create resource script, submit job, and check status

'''

import os
import subprocess
import time



def create_resource_script(python_script, folder, filename, walltime, mem, ncpus, env_variables, SU_project, data_projects, scratch_project):
    command_line = f'python $PYTHON_SCRIPT $PBS_SCRIPT'
    if env_variables:
        command_line = f'{command_line} ' + " ".join([f"${key}" for key, value in env_variables.items()])
    pbs_script = f'{folder}/{filename}_resources.pbs'
    storage_line = "+".join([f"gdata/{proj}" for proj in data_projects] + [f"scratch/{scratch_project}"])
    oe_path = f'{folder}/{filename}.o'
    os.makedirs(folder, exist_ok=True)
    script_content = f"""#!/bin/bash -l
#PBS -S /bin/bash
#PBS -P {SU_project}
#PBS -l storage={storage_line}
#PBS -l wd
#PBS -q normal
#PBS -l walltime={walltime}
#PBS -l mem={mem}
#PBS -l ncpus={ncpus}
#PBS -l jobfs=200GB
#PBS -j oe
#PBS -o {oe_path}

module use /g/data/hh5/public/modules
module load conda/analysis3-unstable
PYTHON_SCRIPT={python_script}
{command_line}"""
    with open(pbs_script, 'w') as file:
        file.write(script_content)
    # print(f'created temp pbs script at: {pbs_script}')
    # gdata/al33+gdata/oi10+gdata/ia39+gdata/rt52+gdata/fs38+gdata/k10+gdata/hh5+scratch/w40
    return pbs_script, oe_path


def submit_job(python_script, folder, filename, walltime, mem, ncpus = str(1), env_variables = {}, SU_project = '', data_projects = [], scratch_project = ''): 
    pbs_script, oe_path = create_resource_script(python_script, folder, filename, walltime, mem, ncpus, env_variables, SU_project, data_projects, scratch_project)
    env_variables[f"PBS_SCRIPT"] = pbs_script
    env_vars_str = ",".join([f"{key}={value}" for key, value in env_variables.items()])
    command = [
        "qsub", 
        "-v", 
        env_vars_str, 
        pbs_script
        ]
    result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)
    job_id = result.stdout.strip()
    print(f'\t\t submitted job\n\t\t oe_file: {oe_path} \n\t\t job ID: {job_id} \n')
    return job_id


def check_qstat(user, delay=5):
    time.sleep(delay)    
    try:
        result = subprocess.run(['qstat', '-u', user], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error:")
        print(e.stderr)






