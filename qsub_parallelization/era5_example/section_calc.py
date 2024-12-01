'''
# -----------------
#  section_calc
# -----------------
Each node executes this script

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
from pathlib import Path
import time
import numpy as np
import pandas as pd
import xarray as xr
import dask.array as dask_array
import shutil

# -- Imported scripts --
sys.path.insert(0, os.getcwd())
import utils.util_qsub.my_settings      as mS


# -- Calculate metric from section of dataset --
def calculate_metric(variable_list):
    dataset, var_name, timescale, year, month, path_folder, file = variable_list
    da_hourly = xr.open_dataset(f'{path_folder}/{file}')[var_name]              # one month of data
    unique_days = pd.to_datetime(da_hourly.time.values).normalize().unique()    # several hours in a day
    print('Creating tempfiles for daily results')
    _, _, _, _, folder_scratch = mS.get_user_specs()
    folder_temp = f'{folder_scratch}/temp_calc/era5_{var_name}_{year}/era5_{var_name}_{year}_{month}'
    if not os.path.exists(folder_temp):
        Path(folder_temp).mkdir(parents=True, exist_ok=True)
    for day in unique_days:
        path_temp = Path(f'{folder_temp}/{var_name}_day_{day}.nc')
        if os.path.exists(path_temp):
            print(f"Results for day {day} already exists. Skipping...")
        else:
            day_str = pd.to_datetime(day).strftime('%Y-%m-%d')
            da_daily_slice = da_hourly.sel(time=slice(day_str, day_str)).load()
            da_daily_slice = - da_daily_slice.resample(time='1D').mean() / (60 * 60 * 24) # convert J/m^2 to W/m^2
            da_daily_slice = da_daily_slice.mean(dim = ('latitude', 'longitude'))
            xr.Dataset({var_name: da_daily_slice}).to_netcdf(path_temp, mode="w")
            print(f'\ttempfile saved in: {path_temp}')
        yield path_temp

# -- save the result from the calculation --
def save_result(variable_list, paths_temp_calc):
    print('Concatenating daily results')
    dataset, var_name, timescale, year, month, path_folder, file = variable_list
    metric_result = xr.open_mfdataset(paths_temp_calc, combine="by_coords")[var_name]
    _, _, _, _, folder_scratch = mS.get_user_specs()
    metric_folder = f'{folder_scratch}/qsub_parallelization_result'
    if not os.path.exists(metric_folder):
        Path(metric_folder).mkdir(parents=True, exist_ok=True)
    path_month = f'{metric_folder}/{dataset}_metricResult_year_{year}_month_{month}.nc'
    xr.Dataset({'metric': metric_result}).to_netcdf(path_month, mode="w")
    print(f'saved metric result at: {path_month}')
    metric_result = xr.open_dataset(path_month)
    print(metric_result)
    temp_folder = os.path.dirname(paths_temp_calc[0])
    print(f'removing temp files from folder: {temp_folder} ..')
    for path_temp in paths_temp_calc:
        os.remove(path_temp)
    print('removing temp folder')
    os.rmdir(temp_folder)
    # print(os.listdir(temp_folder))
    # shutil.rmtree(temp_folder)    # if hidden files appear (not usually the case in scratch)
    return path_month


# -- executed by node --
if __name__ == '__main__':
    if not os.environ.get('PBS_SCRIPT'):                                    # if executed in current terminal (for testing)    
        dataset     = 'era5'   
        var_name    = 'ttr'
        timescale   = 'daily'
        year        = '1999'
        month       = '0'
        path_folder = '/g/data/rt52/era5/single-levels/reanalysis/ttr/1999'
        file        = 'ttr_era5_oper_sfc_19990101-19990131.nc'
        variable_list = [dataset, var_name, timescale, year, month, path_folder, file]
        print(f'-- Getting {dataset} {var_name} {timescale} metric, month: {month} --')
        da_hourly = xr.open_dataset(f'{path_folder}/{file}')[var_name]              # one month of data
        generator = calculate_metric(variable_list)                                 # (if using dask delayed on this step, it outputs execution_maps (futures), to be executed)
        paths_temp_calc = [f for f in generator]
        # metric_result = xr.open_mfdataset(paths_temp_calc, combine="by_coords", engine="netcdf4", parallel=True)  # stop here and you can inspect the temporary files
        path_month = save_result(variable_list, paths_temp_calc)
        print('finished')
    else:                                                                   # if submitted as job
        dataset     = os.environ.get('DATASET')
        var_name    = os.environ.get('VAR_NAME')
        timescale   = os.environ.get('TIMESCALE')
        year        = os.environ.get('YEAR')
        month       = os.environ.get('MONTH')
        path_folder = os.environ.get('PATH_FOLDER')
        file        = os.environ.get('FILE')
        variable_list = [dataset, var_name, timescale, year, month, path_folder, file]
        print(f'-- Getting {dataset} {var_name} {timescale} metric, month: {month} --')
        da_hourly = xr.open_dataset(f'{path_folder}/{file}')[var_name]              # one month of data
        generator = calculate_metric(variable_list)                                 # (if using dask delayed on this step, it outputs execution_maps (futures), to be executed)
        paths_temp_calc = [f for f in generator]
        # metric_result = xr.open_mfdataset(paths_temp_calc, combine="by_coords", engine="netcdf4", parallel=True)  # stop here and you can inspect the temporary files
        path_month = save_result(variable_list, paths_temp_calc)
        print(f'removing pbs script')
        os.remove(os.environ.get('PBS_SCRIPT'))
        print('finished')


