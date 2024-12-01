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

# -- Imported scripts --
sys.path.insert(0, os.getcwd())
import utils.util_qsub.my_settings      as mS


# -- Create high resolution dataset (daskArray) --
def create_daskArray():
    start_date, end_date = "1998-01-01", "2027-12-31"    
    time_range = pd.date_range(start=start_date, end=end_date, freq="D")
    dimensions = {
        'lat':  np.arange(-90, 90, 0.1),        # 0.1 degree resolution (10 km)
        'lon':  np.arange(-180, 180, 0.1),      # 0.1 degree resolution (10 km)
        'time': time_range,                     # daily timesteps       (30 years)
        }
    datapoints = (len(dimensions['lat']) * len(dimensions['lon']) * len(dimensions['time'])) / 10**9
    print(f"High resolution dataset: {np.round(datapoints)} billion datapoints") # :.2e
    # a = np.ones(shape = [int(1e5), int(1e5), int(1e2)])   # does not load with numpy  
    da = dask_array.random.random(size = [len(dimensions['lat']), len(dimensions['lon']), len(dimensions['time'])])
    da = xr.DataArray(da, dims = ['lat', 'lon', 'time'], coords = dimensions) 
    # print(da)
    # exit()
    return da

# -- Calculate metric from section of dataset --
def calculate_metric(variable_list):
    dataset, var_name, timescale, year = variable_list
    da = create_daskArray()
    print(da)
    da_section = da.sel(time = da.time.dt.year == int(year))
    print('\nsection used by node:')
    print(da_section)
    da_section = da_section.load()                                          # might still be too big for login node (create an interactive node for testing)
    metric_result = da_section.mean(dim = ('time', 'lat', 'lon'))
    metric_result = xr.DataArray([metric_result], dims = ['time'], coords = {'time': [pd.Timestamp(f"{year}-01-01")]})
    print(metric_result)
    print('\ncalculated metric')
    return metric_result

# -- save the result from the calculation --
def save_result(dataset, year, metric_result):
    _, _, _, _, folder_scratch = mS.get_user_specs()
    metric_folder = f'{folder_scratch}/qsub_parallelization_result'
    if not os.path.exists(metric_folder):
        Path(metric_folder).mkdir(parents=True, exist_ok=True)
    path_year = f'{metric_folder}/{dataset}_metricResult_{year}.nc'
    xr.Dataset({'metric': metric_result}).to_netcdf(path_year, mode="w")
    print(f'saved metric result at: {path_year}')


# -- executed by node --
if __name__ == '__main__':
    if not os.environ.get('PBS_SCRIPT'):                                    # if executed in current terminal (for testing)    
        dataset     = 'daskArray'   
        var_name    = 'daskArray'
        timescale   = 'daily'
        year        = '1999'
        variable_list = [dataset, var_name, timescale, year]
        metric_result = calculate_metric(variable_list)
        save_result(dataset, year, metric_result)
        print('finished')

    else:                                                                   # if submitted as job
        dataset     = os.environ.get('DATASET')
        var_name    = os.environ.get('VAR_NAME')
        timescale   = os.environ.get('TIMESCALE')
        year        = os.environ.get('YEAR')
        variable_list = [dataset, var_name, timescale, year]
        metric_result = calculate_metric(variable_list)
        save_result(dataset, year, metric_result)
        print(f'removing pbs script')
        os.remove(os.environ.get('PBS_SCRIPT'))
        print('finished')


