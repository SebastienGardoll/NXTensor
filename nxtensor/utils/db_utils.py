# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 11:01:15 2020

@author: sebastien@gardoll.fr
"""
from typing import Dict, Callable, Union, Mapping
import pandas as pd
from enum import Enum

from nxtensor.utils.coordinate_utils import Coordinate
from nxtensor.utils.time_utils import TimeResolution

# [TYPES]


DBMetadataMapping = Dict[Union[Coordinate, TimeResolution], str]


class CsvOptNames(Enum):

    SEPARATOR            = 'sep'
    HEADER               = 'header'
    SAVE_LINE_TERMINATOR = 'line_terminator'
    READ_LINE_TERMINATOR = 'lineterminator'
    ENCODING             = 'encoding'


class DBType(Enum):

    CSV = 'csv'


DEFAULT_CVS_OPTIONS: Mapping[CsvOptNames, Union[str, int]] = {CsvOptNames.SEPARATOR: ',',
                                                              CsvOptNames.HEADER: 0,
                                                              CsvOptNames.ENCODING: 'utf8',
                                                              CsvOptNames.READ_LINE_TERMINATOR: '\\n'}


def load_csv_file(db_file_path: str, options: Mapping[CsvOptNames, Union[str, int]] = DEFAULT_CVS_OPTIONS)\
                  -> pd.DataFrame:
    with open(db_file_path, 'r') as db_file:
        try:
            if options[CsvOptNames.READ_LINE_TERMINATOR] == DEFAULT_CVS_OPTIONS[CsvOptNames.READ_LINE_TERMINATOR]:
                options = {k: v for k, v in options.items()}
                options[CsvOptNames.READ_LINE_TERMINATOR] = '\n'
            result = pd.read_csv(filepath_or_buffer=db_file, **options)
            return result
        except Exception as e:
            msg = f"error while loading cvs file '{db_file_path}' with options {options}"
            raise Exception(msg, e)


def get_dataframe_load_function(db_type: DBType) -> Callable[[str, Dict[CsvOptNames, str]], pd.DataFrame]:
    try:
        return __LOAD_TYPE_FUNCTIONS[db_type]
    except KeyError:
        msg = f"unsupported db type '{db_type}'"
        raise Exception(msg)


__LOAD_TYPE_FUNCTIONS: Mapping[DBType, Callable[[str, Mapping[CsvOptNames, Union[str, int]]], pd.DataFrame]] =\
    {DBType.CSV: load_csv_file}


def create_db_metadata_mapping(lon: str = None, lat: str = None, year: str = None, month: str = None, day: str = None,
                               hour: str = None, minute: str = None, second: str = None, millisecond: str = None,
                               microsecond: str = None) -> DBMetadataMapping:

    result: DBMetadataMapping = DBMetadataMapping()

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
