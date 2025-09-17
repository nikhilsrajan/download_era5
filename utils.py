import os
import cdsapi
import datetime
import time
import xarray as xr
import rioxarray as rio


VARIABLE_TO_NCVAR = {
    "total_precipitation": "tp",
    "2m_temperature": "t2m",
}


def download_data(
    variable : str,
    daily_statistic : str,
    date : datetime.datetime,
    download_filepath : str,
    overwrite : bool = False,
    exist_ok : bool = False,
    url:str = None,
    key:str = None,
    request_full_month:bool = False,
):
    start_time = time.time()

    if os.path.exists(download_filepath) and not overwrite:
        if exist_ok:
            print('File already exists.')
            return 
        raise FileExistsError(f"variable = {variable}, daily_statistic = {daily_statistic}, date = {date.strftime('%Y-%m-%d')}")

    os.makedirs(os.path.dirname(download_filepath), exist_ok=True)

    year = f"{date.year}"
    month = f"{date.month}".zfill(2)

    if not request_full_month:
        days = [f"{date.day}".zfill(2)]
    else:
        days = [f"{d}".zfill(2) for d in range(1, 32)]

    dataset = "derived-era5-single-levels-daily-statistics"
    request = {
        "product_type": "reanalysis",
        "variable": [variable], # ["total_precipitation"]
        "year": year,
        "month": [month],
        "day": days,
        "daily_statistic": daily_statistic, # "daily_sum"
        "time_zone": "utc+00:00",
        "frequency": "1_hourly",
        "format": "netcdf",
    }

    client = cdsapi.Client(url=url, key=key)
    client.retrieve(dataset, request).download(download_filepath)

    end_time = time.time()

    print(f't_elapsed: {end_time - start_time}')


def download_total_precipitation(
    date : datetime.datetime,
    download_filepath : str,
    overwrite : bool = False,
    url: str = None,
    key: str = None,
    request_full_month:bool = False,
    exist_ok : bool = False,
):
    download_data(
        variable = "total_precipitation",
        daily_statistic = "daily_sum",
        date = date,
        download_filepath = download_filepath,
        overwrite = overwrite,
        url = url,
        key = key,
        request_full_month = request_full_month,
        exist_ok = exist_ok,
    )


def download_mean_temperature(
    date : datetime.datetime,
    download_filepath : str,
    overwrite : bool = False,
    url: str = None,
    key: str = None,
    request_full_month:bool = False,
    exist_ok : bool = False,
):
    download_data(
        variable = "2m_temperature",
        daily_statistic = "daily_mean",
        date = date,
        download_filepath = download_filepath,
        overwrite = overwrite,
        url = url,
        key = key,
        request_full_month = request_full_month,
        exist_ok = exist_ok,
    )


def load_mean_temperature_nc_file(nc_filepath:str)->xr.DataArray:
    ncfile = xr.open_dataset(nc_filepath)
    t2m = ncfile['t2m']
    t2m = t2m - 273.15 # K to deg C
    t2m.coords['longitude'] = (t2m.coords['longitude'] + 180) % 360 - 180
    t2m = t2m.sortby('longitude')
    t2m = t2m.rio.set_spatial_dims('longitude', 'latitude')
    t2m = t2m.rio.write_crs('epsg:4326')
    return t2m


def load_total_precipitation_nc_file(nc_filepath:str):
    ncfile = xr.open_dataset(nc_filepath)
    tp = ncfile['tp']
    tp = tp * 1000 # m to mm
    tp.coords['longitude'] = (tp.coords['longitude'] + 180) % 360 - 180
    tp = tp.sortby('longitude')
    tp = tp.rio.set_spatial_dims('longitude', 'latitude')
    tp = tp.rio.write_crs('epsg:4326')
    return tp
