
import os
import subprocess
import time


def create_resource_script(python_script, folder, filename, walltime, mem, ncpus = str(1)):
    pbs_script = f'{folder}/{filename}_resources.pbs'
    oe_path = f'{folder}/{filename}.o'
    os.makedirs(folder, exist_ok=True)
    script_content = f"""#!/bin/bash -l
#PBS -S /bin/bash
#PBS -P k10
#PBS -l storage=gdata/al33+gdata/oi10+gdata/ia39+gdata/rt52+gdata/fs38+gdata/k10+gdata/hh5+scratch/w40
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
python $PYTHON_SCRIPT $PATH_FOLDER $VAR_NAME $VAR_NAME_ERA $YEAR $PBS_SCRIPT"""
    with open(pbs_script, 'w') as file:
        file.write(script_content)
    print(f'\n\t saved resource script at: {pbs_script}')
    return pbs_script

def submit_job(python_script, pbs_script, path_folder, var_name, var_nameERA, year, dataset):
    command = [
        "qsub", 
        "-v", 
        f"PYTHON_SCRIPT={python_script},PATH_FOLDER={path_folder},VAR_NAME={var_name},VAR_NAME_ERA={var_nameERA},YEAR={year},PBS_SCRIPT={pbs_script}", 
        pbs_script
        ]
    result = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True)
    job_id = result.stdout.strip()
    print(f'\t\t submitted job for: {dataset} {var_name}, job ID: {job_id}')

def check_qstat(user, delay=5):
    time.sleep(delay)    
    try:
        result = subprocess.run(['qstat', '-u', user], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error:")
        print(e.stderr)


























def convert_to_era_var_name(var_name):
    # 3D variables (pressure-levels)
    var_nameERA = 'r'       if var_name == 'hur'    else None
    var_nameERA = 'q'       if var_name == 'hus'    else var_nameERA
    var_nameERA = 't'       if var_name == 'ta'     else var_nameERA
    var_nameERA = 'z'       if var_name == 'zg'     else var_nameERA
    var_nameERA = 'w'       if var_name == 'wap'    else var_nameERA
    var_nameERA = 'cc'      if var_name == 'cl'     else var_nameERA

    # 2D variables (single-levels)
    var_nameERA = 'slhf'    if var_name == 'hfls'   else var_nameERA
    var_nameERA = 'sshf'    if var_name == 'hfss'   else var_nameERA

    var_nameERA = 'ttr'     if var_name == 'rlut'   else var_nameERA    # net TOA (no downwelling, so same as outgoing)
    var_nameERA = 'str'     if var_name == 'rls'    else var_nameERA    # net surface (into surface) 
    var_nameERA = 'strd'    if var_name == 'rlds'   else var_nameERA    # downwelling surface
    var_nameERA = 'stru'    if var_name == 'rlus'   else var_nameERA    # upwelling is calculated from net surface and downwelling surface (rlus = rlds - rls)

    var_nameERA = 'tsr'     if var_name == 'rst'    else var_nameERA    # net TOA (into atmosphere)
    var_nameERA = 'tisr'    if var_name == 'rsdt'   else var_nameERA    # downwelling TOA
    var_nameERA = 'tsru'    if var_name == 'rsut'   else var_nameERA    # upwelling is calculated from net TOA and downwelling TOA (rsut = rsdt - rst)
    var_nameERA = 'ssr'     if var_name == 'rss'    else var_nameERA    # net surface (into surface)
    var_nameERA = 'ssrd'    if var_name == 'rsds'   else var_nameERA    # downwelling surface
    var_nameERA = 'ssru'    if var_name == 'rsus'   else var_nameERA    # upwelling is calculated from net surface and downwelling surface (rsus = rsds - rss)

    var_nameERA = '2t'      if var_name == 'tas'    else var_nameERA 
    return var_nameERA




