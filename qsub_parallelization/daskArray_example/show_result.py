'''
# -----------------
#  show_results
# -----------------
Open the results from the files saved by the nodes

'''


# -- Packages --
import os
import sys
import xarray as xr

# -- Imported scripts --
sys.path.insert(0, os.getcwd())
import utils.util_qsub.my_settings      as mS

# -- print / plot results --
if __name__ == '__main__':
    # ds = xr.open_dataset('/scratch/w40/cb4968/qsub_parallelization_result/daskArray_metricResult_1998.nc')
    # print(ds)
    # exit()
    _, _, _, _, folder_scratch = mS.get_user_specs()
    years_range = ['1998-2000']
    year1 = years_range[0].split('-')[0]
    year2 = years_range[0].split('-')[1]
    years = range(int(year1), int(year2)+1)
    dataset = 'daskArray'
    path_years = [f'{folder_scratch}/qsub_parallelization_result/{dataset}_metricResult_{year}.nc' for year in years]
    ds = xr.open_mfdataset(path_years, combine="by_coords", engine="netcdf4", parallel=True)
    print(ds)





