#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 16:14:08 2019

@author: sebastien@gardoll.fr
"""

import math
from nxtensor.utils.coordinates import CoordinateFormat
import numpy as np
import pandas as pd

_CONVERT_MAPPING = dict()


def round_nearest(value, resolution, num_decimal):
    return round(round(value / resolution) * resolution, num_decimal)


def reformat_coordinates(dataframe: pd.DataFrame, column_name: str, from_format: CoordinateFormat,
                         to_format: CoordinateFormat, resolution: float, nb_decimal_to_round: int):
    coordinate_mapping = __get_convert_mapping(from_format, to_format, resolution)

    # Update the format of the coordinate.
    def __convert_coordinates(value):
        rounded_value = round_nearest(value, resolution, nb_decimal_to_round)
        return coordinate_mapping[rounded_value]

    dataframe[column_name] = np.vectorize(__convert_coordinates)(dataframe[column_name])


def __get_convert_mapping(from_format, to_format, resolution):
    if from_format in _CONVERT_MAPPING:
        from_format_dict = _CONVERT_MAPPING[from_format]
        if to_format in from_format_dict:
            to_format_dict = from_format_dict[to_format]
            if resolution in to_format_dict:
                return to_format_dict[resolution]
            else:
                return __compute_mapping(from_format, to_format, resolution, to_format_dict)
        else:
            to_format_dict = dict()
            from_format_dict[to_format] = to_format_dict
            return __compute_mapping(from_format, to_format, resolution, to_format_dict)
    else:
        from_format_dict = dict()
        _CONVERT_MAPPING[from_format] = from_format_dict
        to_format_dict = dict()
        from_format_dict[to_format] = to_format_dict
        return __compute_mapping(from_format, to_format, resolution, to_format_dict)


def __compute_mapping(from_format, to_format, resolution, parent_mapping):
    try:
        result = __GENERATOR[from_format][to_format](resolution)
        parent_mapping[resolution] = result
        return result
    except Exception:
        msg = f"the conversion of coordinates from '{from_format}' to '{to_format}' is not supported"
        raise Exception(msg)


def __generate_mapping_degrees_north_inc_to_dec(resolution):
    values = __generate_float_range(-90, 90, resolution)
    keys   = values
    return dict(zip(keys, values))


def __generate_mapping_degrees_east_m_180_to_0(resolution):
    dest_values = __generate_float_range(0, 360, resolution)
    keys_1 = __generate_float_range(0, (180 + resolution), resolution)
    keys_2 = __generate_float_range((-180 + resolution), 0, resolution)
    result = dict()
    index = 0
    for key in keys_1:
        result[key] = dest_values[index]
        index = index + 1
    for key in keys_2:
        result[key] = dest_values[index]
        index = index + 1
    return result


def __generate_float_range(start, stop, resolution):
    if resolution < 1:
        nb_decimal = len(str(resolution)) - 2  # -2 for the zero and the dot.
        factor = math.pow(10, nb_decimal)
        int_range = range(int(start*factor), int(stop*factor), int(resolution*factor))
        result = list()
        for value in int_range:
            result.append(value/100)
        return result
    else:
        return range(start, stop, resolution)


__GENERATOR = {
    CoordinateFormat.M_180_TO_180_DEGREE_EAST: {
         CoordinateFormat.ZERO_TO_360_DEGREE_EAST: __generate_mapping_degrees_east_m_180_to_0},
    CoordinateFormat.INCREASING_DEGREE_NORTH: {
         CoordinateFormat.DECREASING_DEGREE_NORTH: __generate_mapping_degrees_north_inc_to_dec}
    }


if __name__ == '__main__':
    pass
