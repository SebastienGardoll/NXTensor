#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 10:50:00 2020

@author: sebastien@gardoll.fr
"""
from typing import Mapping

import dask
import xarray as xr
import nxtensor.utils.coordinate_utils as coordinate_utils
from nxtensor.exceptions import ExtractionError


def open_netcdf(netcdf_file_path: str, options: Mapping[str, str] = None) -> xr.Dataset:
    if options is None:
        options = {}
    return xr.open_dataset(netcdf_file_path, **options)


# Extract the region that centers the given lat/lon location.
def extract_square_region(dataset: xr.Dataset, variable_netcdf_attr_name: str, formatted_date: str,
                          lat: float, lat_resolution: float, half_lat_frame: int,
                          lon: float, lon_resolution: float, half_lon_frame: int,
                          variable_level: int = None, level_netcdf_attr_name: str = 'level',
                          time_netcdf_attr_name: str = 'time',
                          lat_netcdf_attr_name: str = 'latitude',
                          lon_netcdf_attr_name: str = 'longitude',
                          has_to_round: bool = False, lat_nb_decimal: int = None, lon_nb_decimal: int = None,
                          dask_scheduler: str = 'single-threaded') -> xr.DataArray:
    if has_to_round:
        if (not lat_nb_decimal) or (not lon_nb_decimal):
            raise ExtractionError("when has_to_round is true, lat_nb_decimal and lon_nb_decimal must be provided")
        lat = coordinate_utils.round_nearest(lat, lat_resolution, lat_nb_decimal)
        lon = coordinate_utils.round_nearest(lon, lon_resolution, lon_nb_decimal)

    # Minus lat_resolution because the upper bound in slice is included.
    lat_min = (lat - half_lat_frame + lat_resolution)
    lat_max = (lat + half_lat_frame)
    # Minus lon_resolution because the upper bound in slice is included.
    lon_min = (lon - half_lon_frame)
    lon_max = (lon + half_lon_frame - lon_resolution)

    lat_series = dataset[lat_netcdf_attr_name]

    # Switching lat min and max.
    if lat_series[0] > lat_series[-1]:
        tmp = lat_min
        lat_min = lat_max
        lat_max = tmp
        del tmp

    indexers = dict()
    indexers[time_netcdf_attr_name] = formatted_date
    indexers[lat_netcdf_attr_name] = slice(lat_min, lat_max)
    indexers[lon_netcdf_attr_name] = slice(lon_min, lon_max)

    with dask.config.set(scheduler=dask_scheduler):
        if variable_level:
            indexers[level_netcdf_attr_name] = variable_level

        result = dataset[variable_netcdf_attr_name].sel(indexers=indexers).compute()
        # Drop the coordinates (time, latitude and longitude) so as to concatenate
        # the extracted region so as to stack several of them and make a channel.
        result = result.drop(result.coords)
        return result


def __era5_unit_test_extraction(variable_name: str,
                                year: int, month: int, day: int, hour: int,
                                lat: float, lon: float,
                                variable_level: int = None) -> xr.DataArray:
    from matplotlib import pyplot as plt

    # Era5 variable settings.
    half_lat_frame = 4
    half_lon_frame = 4
    lat_resolution = 0.25
    lon_resolution = 0.25
    lat_nb_decimal = 2
    lon_nb_decimal = 2
    month_2_digit = f"{month:02d}"
    hour_2_digit = f"{hour:02d}"
    formatted_date = f'{year}-{month_2_digit}-{day}T{hour_2_digit}'
    if variable_level:
        netcdf_file_path = f'/bdd/ERA5/NETCDF/GLOBAL_025/4xdaily/AN_PL/{year}/{variable_name}.{year}{month_2_digit}' +\
            '.aphe5.GLOBAL_025.nc'
    else:
        netcdf_file_path = f'/bdd/ERA5/NETCDF/GLOBAL_025/hourly/AN_SF/{year}/{variable_name}.{year}{month_2_digit}' +\
            '.as1e5.GLOBAL_025.nc'

    print(f"> opening '{netcdf_file_path}'")
    with open_netcdf(netcdf_file_path) as dataset:
        print("> extracting region")
        extracted_region = extract_square_region(dataset=dataset, variable_netcdf_attr_name=variable_name,
                                                 formatted_date=formatted_date, variable_level=variable_level,
                                                 lat=lat, lat_resolution=lat_resolution, half_lat_frame=half_lat_frame,
                                                 lon=lon, lon_resolution=lon_resolution, half_lon_frame=half_lon_frame,
                                                 has_to_round=True, lat_nb_decimal=lat_nb_decimal,
                                                 lon_nb_decimal=lon_nb_decimal)
        print("> displaying region")
        plt.figure()
        plt.imshow(extracted_region, cmap='gist_rainbow_r', interpolation="none")
        plt.show()
        return extracted_region


def __unit_test1():
    variable_name = 'msl'
    year = 2000
    month = 10
    day = 1
    hour = 0
    lat = 39.7
    lon = 312  # Equivalent to -48 .
    return __era5_unit_test_extraction(variable_name, year, month, day, hour, lat, lon)


def __unit_test2():
    variable_name = 'ta'
    variable_level = 200
    year = 2011
    month = 8
    day = 25
    hour = 18
    lat = 26.5
    lon = 282.8  # Equivalent to -77.2 .
    return __era5_unit_test_extraction(variable_name, year, month, day, hour, lat, lon, variable_level)


if __name__ == '__main__':
    __unit_test1()
    __unit_test2()
