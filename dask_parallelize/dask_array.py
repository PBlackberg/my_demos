
import numpy as np
import xarray as xr
import dask.array as dask_array
import dask


# a = np.ones(shape = [int(1e5), int(1e5), int(1e2)]) # 1 trillion data points (does not load)
# print(a)       
da = dask_array.random.random(size = [int(1e5), int(1e5), int(1e2)])
da = xr.DataArray(da, dims = ['x', 'y', 'z']) 
print(da)

da_section = da.isel(x = slice(0,1000), y = slice(0,1000))
da_section = da_section.load()
print(da_section)




