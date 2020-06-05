# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 11:01:15 2020

@author: sebastien@gardoll.fr
"""
from typing import Dict, Callable, Union, Mapping
import pandas as pd

from nxtensor.utils.coordinates import Coordinate
from nxtensor.utils.csv_option_names import CsvOptName
from nxtensor.utils.csv_utils import DEFAULT_CSV_OPTIONS
from nxtensor.utils.db_types import DBType
from nxtensor.utils.time_resolutions import TimeResolution

# [TYPES]


DBMetadataMapping = Dict[Union[Coordinate, TimeResolution], str]


def save_to_csv_file(data: pd.DataFrame, csv_file_path: str, options: Mapping[CsvOptName, any] = DEFAULT_CSV_OPTIONS):
    try:
        # Line terminator parameter name is not compatible with pandas.to_csv.
        options = {k: v for k, v in options.items()}
        line_terminator = options[CsvOptName.LINE_TERMINATOR]
        del options[CsvOptName.LINE_TERMINATOR]
        options['line_terminator'] = line_terminator

        data.to_csv(path_or_buf=csv_file_path, **options)
    except Exception as e:
        msg = f"error while saving cvs file '{csv_file_path}' with options {options}"
        raise Exception(msg, e)


def load_csv_file(csv_file_path: str, options: Mapping[CsvOptName, any] = DEFAULT_CSV_OPTIONS)\
                  -> pd.DataFrame:
    with open(csv_file_path, 'r') as db_file:
        try:
            result = pd.read_csv(filepath_or_buffer=db_file, **options)
            return result
        except Exception as e:
            msg = f"error while loading cvs file '{csv_file_path}' with options {options}"
            raise Exception(msg, e)


def get_dataframe_load_function(db_type: DBType) -> Callable[[str, Mapping[CsvOptName, any]], pd.DataFrame]:
    try:
        return __LOAD_TYPE_FUNCTIONS[db_type]
    except KeyError:
        msg = f"unsupported db type '{db_type}'"
        raise Exception(msg)


__LOAD_TYPE_FUNCTIONS: Mapping[DBType, Callable[[str, Mapping[CsvOptName, any]], pd.DataFrame]] =\
    {DBType.CSV: load_csv_file}


def create_db_metadata_mapping(lon: str = None, lat: str = None, year: str = None, month: str = None, day: str = None,
                               hour: str = None, minute: str = None, second: str = None, millisecond: str = None,
                               microsecond: str = None) -> DBMetadataMapping:

    result: DBMetadataMapping = dict()

    if lat:
        result[Coordinate.LAT] = lat

    if lon:
        result[Coordinate.LON] = lon

    if year:
        result[TimeResolution.YEAR] = year

    if month:
        result[TimeResolution.MONTH] = month

    if day:
        result[TimeResolution.DAY] = day

    if hour:
        result[TimeResolution.HOUR] = hour

    if minute:
        result[TimeResolution.MINUTE] = minute

    if second:
        result[TimeResolution.SECOND] = second

    if millisecond:
        result[TimeResolution.MILLISECOND] = millisecond

    if microsecond:
        result[TimeResolution.MICROSECOND] = microsecond

    return result
