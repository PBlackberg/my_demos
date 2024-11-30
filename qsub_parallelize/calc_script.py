
import os
import xarray as xr
from pathlib import Path
import util_parallel.dask_funcs as dF

import os
import sys
sys.path.insert(0, os.getcwd())
import utils.util_files.save_folders    as sF



if __name__ == '__main__':
    path_folder = os.environ.get('PATH_FOLDER')
    var_name    = os.environ.get('VAR_NAME')
    var_nameERA = os.environ.get('VAR_NAME_ERA')
    year        = os.environ.get('YEAR')
    # get_era5_year(var_name, var_nameERA, path_folder, year)

    print(year)
    print(var_name)
    print(path_folder)

    if year == '1998':
        exit()

    if os.environ.get('PBS_SCRIPT'):
        print(f'removing pbs script')
        os.remove(os.environ.get('PBS_SCRIPT'))
    print('finished')




