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
    # ds = xr.open_dataset('/scratch/w40/cb4968/qsub_parallelization_result/era5_metricResult_year_1999_month_0.nc')
    # print(ds)
    # exit()
    _, _, _, _, folder_scratch = mS.get_user_specs()
    dataset = 'era5'
    years_range = ['1999-1999']
    year1 = years_range[0].split('-')[0]
    year2 = years_range[0].split('-')[1]
    years = range(int(year1), int(year2)+1)
    months = range(0, 12)
    for year in years:
        metric_folder = f'{folder_scratch}/qsub_parallelization_result'
        path_months = [f'{metric_folder}/{dataset}_metricResult_year_{year}_month_{month}.nc' for month in months if os.path.isfile(f'{metric_folder}/{dataset}_metricResult_year_{year}_month_{month}.nc')]
        ds = xr.open_mfdataset(path_months, combine="by_coords", engine="netcdf4", parallel=True)
        print(ds)

