#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 11:58:46 2019

@author: sebastien@gardoll.fr
"""

import os.path as path
from nxtensor.utils.time_resolutions import TimeResolution
from nxtensor.variable import SingleLevelVariable, MultiLevelVariable, Variable, ComputedVariable


def bootstrap_era5_variables(variable_parent_dir_path: str) -> None:
    era5_single_level_variables = ['msl', 'tcwv', 'u10', 'v10']
    time_resolution = TimeResolution.HOUR
    netcdf_period_resolution = TimeResolution.MONTH

    for str_id in era5_single_level_variables:
        netcdf_path_template = '/bdd/ERA5/NETCDF/GLOBAL_025/hourly/AN_SF/{year}/%s.{year}{month2d}.as1e5.GLOBAL_025.nc'\
                               % str_id
        __bootstrap_era5_variable(str_id, str_id, netcdf_path_template, time_resolution, netcdf_period_resolution,
                                  variable_parent_dir_path)

    era5_multi_level_variables = [('ta200', 'ta', 200), ('ta500', 'ta', 500),
                                  ('u850', 'u', 850), ('v850', 'v', 850)]
    time_resolution = TimeResolution.HOUR
    for str_id, attr_name, level in era5_multi_level_variables:
        netcdf_path_template = '/bdd/ERA5/NETCDF/GLOBAL_025/4xdaily/AN_PL/{year}/%s.{year}{month2d}' % attr_name + \
                               '.aphe5.GLOBAL_025.nc'
        __bootstrap_era5_variable(str_id, attr_name, netcdf_path_template, time_resolution, netcdf_period_resolution,
                                  variable_parent_dir_path, level, 'level')


def __bootstrap_era5_variable(str_id: str, attribute_name: str, netcdf_path_template: str,
                              time_resolution: TimeResolution, netcdf_period_resolution: TimeResolution,
                              variable_parent_dir_path: str, level: int = None, level_netcdf_attr_name: str = None)\
                              -> None:
    if level is None:
        variable = SingleLevelVariable(str_id)
    else:
        variable = MultiLevelVariable(str_id)
        variable.level = level
        variable.level_netcdf_attr_name = level_netcdf_attr_name

    variable.netcdf_attr_name = attribute_name
    variable.time_resolution = time_resolution
    variable.time_netcdf_attr_name = 'time'
    variable.netcdf_period_resolution = netcdf_period_resolution
    variable.date_template = '{year}-{month2d}-{day}T{hour2d}'
    variable.netcdf_path_template = netcdf_path_template
    variable.lat_resolution = 0.25
    variable.lat_nb_decimal = 2
    variable.lat_netcdf_attr_name = 'latitude'
    variable.lon_resolution = 0.25
    variable.lon_nb_decimal = 2
    variable.lon_netcdf_attr_name = 'longitude'
    variable_file_path = path.join(variable_parent_dir_path, variable.compute_filename())
    variable.save(variable_file_path)


def test_load(variable_parent_dir_path: str) -> None:
    era5_variables = ['msl', 'tcwv', 'u10', 'v10', 'ta200', 'ta500', 'u850', 'v850', 'wsl10', 'dummy']
    for str_id in era5_variables:
        var = Variable.load(path.join(variable_parent_dir_path, Variable.generate_filename(str_id)))
        print(var)


def create_computed_variables(variable_parent_dir_path: str) -> None:
    variable = ComputedVariable('wsl10')
    variable.computation_expression = 'u10 2 pow v10 2 pow + sqrt'
    variable.variable_file_paths = [path.join(variable_parent_dir_path,
                                              f"u10_{Variable.FILE_NAME_POSTFIX}"),
                                    path.join(variable_parent_dir_path,
                                              f"v10_{Variable.FILE_NAME_POSTFIX}")]

    variable.netcdf_period_resolution = TimeResolution.MONTH
    variable.time_resolution = TimeResolution.HOUR
    variable.date_template = '{year}-{month2d}-{day}T{hour2d}'
    variable.lat_nb_decimal = 2
    variable.lat_resolution = 0.25
    variable.lon_nb_decimal = 2
    variable.lon_resolution = 0.25
    variable_file_path = path.join(variable_parent_dir_path, variable.compute_filename())
    variable.save(variable_file_path)

    # Recursive computed variable.
    variable = ComputedVariable('dummy')
    variable.computation_expression = 'u10 wsl10 + 2 /'
    variable.variable_file_paths = [path.join(variable_parent_dir_path,
                                              f"u10_{Variable.FILE_NAME_POSTFIX}"),
                                    path.join(variable_parent_dir_path,
                                              f"wsl10_{Variable.FILE_NAME_POSTFIX}")]

    variable.netcdf_period_resolution = TimeResolution.MONTH
    variable.time_resolution = TimeResolution.HOUR
    variable.date_template = '{year}-{month2d}-{day}T{hour2d}'
    variable.lat_nb_decimal = 2
    variable.lat_resolution = 0.25
    variable.lon_nb_decimal = 2
    variable.lon_resolution = 0.25
    variable_file_path = path.join(variable_parent_dir_path, variable.compute_filename())
    variable.save(variable_file_path)


def bootstrap_all(config_file_parent_dir_path: str) -> None:
    bootstrap_era5_variables(config_file_parent_dir_path)
    create_computed_variables(config_file_parent_dir_path)


def __all_tests():
    config_files_parent_dir_path = '/home/sgardoll/extraction_config'
    bootstrap_all(config_files_parent_dir_path)
    test_load(config_files_parent_dir_path)


if __name__ == '__main__':
    __all_tests()
