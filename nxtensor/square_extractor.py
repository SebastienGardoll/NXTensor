#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 17:00:00 2020

@author: sebastien@gardoll.fr
"""
from typing import Dict, Union, Mapping

import xarray as xr
import nxtensor.core.xarray_extractions as xtract
from nxtensor.utils.coordinates import Coordinate
from nxtensor.utils.time_resolutions import TimeResolution
from nxtensor.utils.xarray_rpn_calulator import XarrayRpnCalculator
from nxtensor.variable import MultiLevelVariable, SingleLevelVariable, ComputedVariable, VariableVisitor, \
    VariableNetcdfFilePathVisitor, Variable, VariableId


class SquareRegionExtractionVisitor(VariableVisitor):

    def __init__(self, datasets: Mapping[VariableId, xr.Dataset],
                 extraction_data: Mapping[Union[Coordinate, TimeResolution], Union[int, float]],
                 half_lat_frame: int, half_lon_frame: int, dask_scheduler: str = 'single-threaded'):
        # Buffer of extracted regions: optimization for computed variables.
        # Computed variables may contain computed variables, recursively !
        self.__extracted_regions: Dict[VariableId, xr.DataArray] = dict()
        self.__datasets: Mapping[VariableId, xr.Dataset] = datasets
        self.__extraction_data: Mapping[Union[Coordinate, TimeResolution], Union[int, float]] = extraction_data
        self.__half_lat_frame: int = half_lat_frame
        self.__half_lon_frame: int = half_lon_frame
        self.__dask_scheduler: str = dask_scheduler
        self.__recursive_call_count: int = 0
        # noinspection PyTypeChecker
        self.result: xr.DataArray = None

    def __bootstrap(self, var: SingleLevelVariable) -> str:
        # month2d, day2d and hour2d are computed when calling convert_block_to_dict function from module
        # xarray_channel_extraction.
        formatted_date = var.date_template.format(**self.__extraction_data)
        return formatted_date

    def visit_single_level_variable(self, var: SingleLevelVariable) -> None:
        if var.str_id not in self.__extracted_regions:
            formatted_date = self.__bootstrap(var)
            self.result = xtract.extract_square_region(dataset=self.__datasets[var.str_id],
                                                       variable_netcdf_attr_name=var.netcdf_attr_name,
                                                       formatted_date=formatted_date,
                                                       lat=self.__extraction_data[Coordinate.LAT],
                                                       lat_resolution=var.lat_resolution,
                                                       half_lat_frame=self.__half_lat_frame,
                                                       lon=self.__extraction_data[Coordinate.LON],
                                                       lon_resolution=var.lon_resolution,
                                                       half_lon_frame=self.__half_lon_frame,
                                                       time_netcdf_attr_name=var.time_netcdf_attr_name,
                                                       lat_netcdf_attr_name=var.lat_netcdf_attr_name,
                                                       lon_netcdf_attr_name=var.lon_netcdf_attr_name,
                                                       has_to_round=True, lat_nb_decimal=var.lat_nb_decimal,
                                                       lon_nb_decimal=var.lon_nb_decimal,
                                                       dask_scheduler=self.__dask_scheduler)
            self.__extracted_regions[var.str_id] = self.result

    def visit_multi_level_variable(self, var: MultiLevelVariable) -> None:
        if var.str_id not in self.__extracted_regions:
            formatted_date = self.__bootstrap(var)
            self.result = xtract.extract_square_region(dataset=self.__datasets[var.str_id],
                                                       variable_netcdf_attr_name=var.netcdf_attr_name,
                                                       formatted_date=formatted_date,
                                                       lat=self.__extraction_data[Coordinate.LAT],
                                                       lat_resolution=var.lat_resolution,
                                                       half_lat_frame=self.__half_lat_frame,
                                                       lon=self.__extraction_data[Coordinate.LON],
                                                       lon_resolution=var.lon_resolution,
                                                       half_lon_frame=self.__half_lon_frame, variable_level=var.level,
                                                       level_netcdf_attr_name=var.level_netcdf_attr_name,
                                                       time_netcdf_attr_name=var.time_netcdf_attr_name,
                                                       lat_netcdf_attr_name=var.lat_netcdf_attr_name,
                                                       lon_netcdf_attr_name=var.lon_netcdf_attr_name,
                                                       has_to_round=True, lat_nb_decimal=var.lat_nb_decimal,
                                                       lon_nb_decimal=var.lon_nb_decimal,
                                                       dask_scheduler=self.__dask_scheduler)
            self.__extracted_regions[var.str_id] = self.result

    def visit_computed_variable(self, var: ComputedVariable) -> None:
        if var.str_id not in self.__extracted_regions:
            self.__recursive_call_count = self.__recursive_call_count + 1
            for internal_var in var.get_variables().values():
                internal_var.accept(self)

            calculator = XarrayRpnCalculator(var.computation_expression, self.__extracted_regions,
                                             self.__dask_scheduler)
            self.result = calculator.compute()
            self.__extracted_regions[var.str_id] = self.result
            self.__recursive_call_count = self.__recursive_call_count - 1

        if self.__recursive_call_count == 0:
            # Flush tmp data from memory. As computed variables may contain computed variables, recursively,
            # dumping tmp data will be under optimized if this call of visit_computed_variable is not the last call
            # on the recursive stack. It's ok to not flush __extracted_regions for simple and multilevel var.
            self.__extracted_regions.clear()

    def get_result(self) -> xr.DataArray:
        self.__extracted_regions.clear()
        return self.result


# TODO: recursive computed variable with common variables between the nested computed variables.


def __unit_test_create_extraction_data(lat: float, lon: float, year: int, month: int, day: int, hour: int) \
                             -> Mapping[Union[Coordinate, TimeResolution], Union[int, float]]:
    result = {Coordinate.LAT: lat, Coordinate.LON: lon, TimeResolution.YEAR: year, TimeResolution.MONTH: month,
              TimeResolution.MONTH2D: f"{month:02d}", TimeResolution.DAY: day, TimeResolution.DAY2D: f"{day:02d}",
              TimeResolution.HOUR: hour, TimeResolution.HOUR2D: f"{hour:02d}"}
    return result


def __unit_test_computed_variable() -> None:
    lat_frame = 8
    lon_frame = 8
    variable_parent_dir_path = '/home/sgardoll/extraction_config'

    str_id    = 'wsl10'
    year      = 2000
    month     = 10
    day       = 1
    hour      = 0
    lat       = 39.7
    lon       = 312  # Equivalent to -48 .
    extraction_data = __unit_test_create_extraction_data(lat, lon, year, month, day, hour)

    extracted_region = __unit_test_extraction(str_id, variable_parent_dir_path, extraction_data,
                                              lat_frame, lon_frame)
    # DEBUG
    print(extracted_region.shape)


def __unit_test_single_multi_level() -> None:
    lat_frame = 8
    lon_frame = 8
    variable_parent_dir_path = '/home/sgardoll/extraction_config'

    str_id = 'msl'
    year   = 2000
    month  = 10
    day    = 1
    hour   = 0
    lat    = 39.7
    lon    = 312  # Equivalent to -48 .
    extraction_data = __unit_test_create_extraction_data(lat, lon, year, month, day, hour)
    __unit_test_extraction(str_id, variable_parent_dir_path, extraction_data, lat_frame, lon_frame)

    str_id = 'ta200'
    year   = 2011
    month  = 8
    day    = 25
    hour   = 18
    lat    = 26.5
    lon    = 282.8  # Equivalent to -77.2 .
    extraction_data = __unit_test_create_extraction_data(lat, lon, year, month, day, hour)
    __unit_test_extraction(str_id, variable_parent_dir_path, extraction_data, lat_frame, lon_frame)

    str_id = 'msl'
    year   = 2011
    month  = 8
    day    = 21
    hour   = 0
    lat    = 15
    lon    = 301  # Equivalent to -59 .
    extraction_data = __unit_test_create_extraction_data(lat, lon, year, month, day, hour)
    __unit_test_extraction(str_id, variable_parent_dir_path, extraction_data, lat_frame, lon_frame)


def __unit_test_extraction(str_id, variable_parent_dir_path,
                           extraction_data: Mapping[Union[Coordinate, TimeResolution], Union[int, float]],
                           half_lat_frame, half_lon_frame, has_to_plot=True):
    import os.path as path
    from matplotlib import pyplot as plt
    import nxtensor.utils.file_utils as fu
    var = Variable.load(path.join(variable_parent_dir_path,
                        f"{str_id}{fu.NAME_SEPARATOR}{Variable.FILE_NAME_POSTFIX}"))

    # noinspection PyTypeChecker
    visitor = VariableNetcdfFilePathVisitor(extraction_data)
    var.accept(visitor)
    netcdf_file_path_dict = visitor.get_result()
    datasets = {var_id: xtract.open_netcdf(netcdf_file_path) for var_id, netcdf_file_path in netcdf_file_path_dict.items()}
    extractor = SquareRegionExtractionVisitor(datasets=datasets, extraction_data=extraction_data,
                                              half_lat_frame=half_lat_frame, half_lon_frame=half_lon_frame,
                                              dask_scheduler='single-threaded')
    var.accept(extractor)
    extracted_region = extractor.result
    [dataset.close() for dataset in datasets.values()]

    if has_to_plot:
        plt.figure()
        plt.imshow(extracted_region, cmap='gist_rainbow_r', interpolation="none")
        plt.show()
    return extracted_region


if __name__ == '__main__':
    # __unit_test_single_multi_level()
    __unit_test_computed_variable()
