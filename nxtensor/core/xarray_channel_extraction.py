#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  22 09:20:10 2020

@author: sebastien@gardoll.fr
"""

from abc import abstractmethod, ABC
from typing import Dict, Set, Tuple, Mapping, Sequence, List

import pandas as pd
import xarray as xr

import nxtensor.utils.csv_utils
import nxtensor.utils.hdf5_utils
from nxtensor.exceptions import ConfigurationError
from nxtensor.utils.coordinates import Coordinate
from nxtensor.utils.time_resolutions import TimeResolution
from nxtensor.utils.db_utils import create_db_metadata_mapping
from nxtensor.utils.csv_option_names import CsvOptName

from multiprocessing import Pool
import os.path as path
import os

import nxtensor.utils.time_utils as tu
import nxtensor.utils.naming_utils as nu
import nxtensor.utils.csv_utils as cu
import nxtensor.utils.db_utils as du

import pickle

import time

from nxtensor.core.types import VariableId, LabelId, MetaDataBlock, Period, DBMetadataMapping

import numpy as np

INDEX_NAME = 'index'


METADATA_TYPES = {TimeResolution.DAY: np.int8, TimeResolution.DAY2D: np.str,
                  TimeResolution.HOUR: np.int8, TimeResolution.HOUR2D: np.str,
                  TimeResolution.MONTH: np.int8, TimeResolution.MONTH2D: np.str,
                  TimeResolution.YEAR: np.int16,
                  Coordinate.LAT: float, Coordinate.LON: float}


class BlockProcessor(ABC):

    @abstractmethod
    def process_blocks(self, period: Period, extraction_metadata_blocks: List[Tuple[LabelId, MetaDataBlock]]) \
            -> Tuple[str, List[Tuple[LabelId, xr.DataArray, MetaDataBlock]]]:
        pass


def convert_block_to_dict(extraction_metadata_block: pd.DataFrame) -> MetaDataBlock:
    result = extraction_metadata_block.to_dict('records')
    for dictionary in result:
        dictionary[TimeResolution.MONTH2D] = f"{int(dictionary[TimeResolution.MONTH]):02d}"
        dictionary[TimeResolution.DAY2D] = f"{int(dictionary[TimeResolution.DAY]):02d}"
        dictionary[TimeResolution.HOUR2D] = f"{int(dictionary[TimeResolution.HOUR]):02d}"
    return result


def preprocess_extraction(preprocessing_output_file_path: str,
                          extraction_metadata_blocks: Mapping[LabelId, pd.DataFrame],
                          db_metadata_mappings: Mapping[LabelId, DBMetadataMapping],
                          netcdf_file_time_period: TimeResolution,
                          inplace: bool = False):
    # Compute the extraction_metadata_blocks according to the label for all the period of time.
    structures = dict()
    for label_id, dataframe in extraction_metadata_blocks.items():
        db_metadata_mapping = db_metadata_mappings[label_id]
        structure = __build_blocks_structure(dataframe, db_metadata_mapping, netcdf_file_time_period, inplace)
        structures[label_id] = structure

    # Merged_structures guarantees the order (following period, label_id and extraction metadata).
    merged_structures: List[Tuple[Period, List[Tuple[LabelId, MetaDataBlock]]]] = __merge_block_structures(structures)
    del structures

    os.makedirs(path.dirname(preprocessing_output_file_path), exist_ok=True)
    try:
        with open(preprocessing_output_file_path, 'wb') as file:
            pickle.dump(obj=merged_structures, file=file, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        msg = f'> [ERROR] unable to persist extraction preprocessing in {preprocessing_output_file_path}'
        raise Exception(msg, e)


def extract(variable_id: str,
            preprocess_input_file_path: str,
            block_processor: BlockProcessor,
            extraction_metadata_block_csv_save_options: Mapping[CsvOptName, any] = None,
            nb_workers: int = 1) -> Dict[Period, Dict[str, Dict[str, str]]]:

    # Returns the extraction data and extraction metadata blocks file paths.
    try:
        with open(preprocess_input_file_path, 'rb') as file:
            merged_structures = pickle.load(file=file)
    except Exception as e:
        msg = f'unable to load extraction preprocessing located at {preprocess_input_file_path}'
        raise Exception(msg, e)

    static_parameters = (variable_id, block_processor,
                         extraction_metadata_block_csv_save_options)
    parameters_list = [(period, extraction_metadata_blocks, *static_parameters)
                       for period, extraction_metadata_blocks in merged_structures]
    start = time.time()
    if nb_workers > 1:
        print(f"> variable {variable_id} starting parallel extractions (number of workers: {nb_workers})")
        with Pool(processes=nb_workers) as pool:
            tmp_result = pool.map(func=__map_core_extraction, iterable=parameters_list, chunksize=1)
    else:
        print(f"> variable {variable_id} starting sequential extractions")
        for parameters in parameters_list:
            tmp_result = __map_core_extraction(parameters)
    print(f"> elapsed time: {tu.display_duration(time.time()-start)}")
    result = dict(tmp_result)
    return result


def __map_core_extraction(parameters):
    return __core_extraction(*parameters)


def __core_extraction(period: Period,
                      extraction_metadata_blocks: List[Tuple[LabelId, MetaDataBlock]],
                      variable_id: VariableId,
                      block_processor: BlockProcessor,
                      extraction_metadata_block_csv_save_options: Mapping[CsvOptName, any] = None)\
                      -> Tuple[Period, Dict[str, Dict[str, str]]]:
    result: Dict[str, Dict[str, str]] = dict()
    parent_dir_path, extracted_data_blocks = block_processor.process_blocks(period, extraction_metadata_blocks)

    for extracted_data_block in extracted_data_blocks:
        label_id, data_block, metadata_block = extracted_data_block
        period_str = nu.create_period_str(period)
        data_metadata_parent_dir = path.join(parent_dir_path, f"{period_str}", label_id)
        os.makedirs(data_metadata_parent_dir, exist_ok=True)
        data_block_file_path, metadata_block_file_path = \
            nu.compute_data_meta_data_file_path(variable_id, data_metadata_parent_dir)
        if extraction_metadata_block_csv_save_options is None:
            cu.to_csv(data=metadata_block, file_path=metadata_block_file_path)
        else:
            cu.to_csv(data=metadata_block, file_path=metadata_block_file_path,
                      csv_options=extraction_metadata_block_csv_save_options)

        nxtensor.utils.hdf5_utils.write_ndarray_to_hdf5(data_block_file_path, data_block.values)
        print(f'> saved {label_id} data block (shape: {data_block.shape}) for period {period_str}')
        result[label_id] = dict()
        result[label_id]['data_block'] = data_block_file_path
        result[label_id]['metadata_block'] = metadata_block_file_path
    return period, result


# Enable processing of extractions period by period so as to open a netcdf file only one time.
# The returned data structure is ordered following the Period and the LabelId.
def __merge_block_structures(structures: Mapping[LabelId, Dict[Period, MetaDataBlock]])\
        -> List[Tuple[Period, List[Tuple[LabelId, MetaDataBlock]]]]:
    # str for label_id.
    # Build a set of periods.
    # (like (year, month), e.g. (2000, 10)).
    periods: Set[Period] = set()
    for structure in structures.values():
        periods.update(structure.keys())

    periods: Sequence[Period] = tu.sort_periods(periods)
    label_ids = nu.sort_labels(structures.keys())

    result: List[Tuple[Period, List[Tuple[LabelId, MetaDataBlock]]]] = list()

    for period in periods:
        current_blocks: List[Tuple[LabelId, MetaDataBlock]] = list()
        for label_id in label_ids:
            if period in structures[label_id]:
                current_blocks.append((label_id, structures[label_id][period]))
        result.append((period, current_blocks))

    return result


def __build_blocks_structure(dataframe: pd.DataFrame, db_metadata_mapping: DBMetadataMapping,
                             netcdf_file_time_period: TimeResolution, inplace=False) \
                             -> Dict[Period, MetaDataBlock]:
    # Return the dataframe grouped by the given period covered by the netcdf file.
    # The result is a dictionary of extraction_metadata_blocks (rows of the given dataframe) mapped with a
    # period (a tuple of time attributes).
    # It also renames (inplace or not) the name of the columns of the dataframe, according
    # to the given db_metadata_mapping which has to be generate by the time_utils.create_db_metadata_mapping function.
    # Example :
    # if the netcdf file covers a month of data, the period is the month.
    # This function returns the dataframe grouped by period of (year, month).
    # (2000, 1)
    #     - dataframe row at index 6
    #     - dataframe row at index 19
    #     - dataframe row at index 21
    #     ...
    # (2000, 2)
    #     - dataframe row at index 2
    #     - dataframe row at index 7
    #     ...
    #  ...
    try:
        resolution_degree = TimeResolution.KEYS.index(netcdf_file_time_period)
    except ValueError as e:
        msg = f"'{netcdf_file_time_period}' is not a known time resolution"
        raise ConfigurationError(msg, e)

    list_keys = TimeResolution.KEYS[0:(resolution_degree + 1)]
    list_column_names = [db_metadata_mapping[key] for key in list_keys]
    indices = dataframe.groupby(list_column_names).indices

    # Rename the columns of the dataframe.
    reverse_metadata_mapping = {v: k for k, v in db_metadata_mapping.items()}
    if inplace:
        dataframe.rename(reverse_metadata_mapping, axis='columns', inplace=True)
        renamed_df = dataframe
    else:
        renamed_df = dataframe.rename(reverse_metadata_mapping, axis='columns', inplace=False)

    renamed_df.index.name = INDEX_NAME
    # Select only the columns of interest (lat, lon, year, etc.).
    restricted_renamed_df = renamed_df[db_metadata_mapping.keys()]

    # Compute the extraction_metadata_blocks.
    result: Dict[Period, MetaDataBlock] = dict()
    for index in indices.keys():
        result[index] = convert_block_to_dict(restricted_renamed_df.loc[indices[index]])

    return result


def __test_build_blocks_structure(csv_file_path: str, period_resolution: TimeResolution)\
        -> Dict[Period, MetaDataBlock]:
    dataframe = du.load_csv_file(csv_file_path)
    db_metadata_mapping = create_db_metadata_mapping(year='year', month='month', day='day', hour='hour',
                                                     lat='lat', lon='lon')
    return __build_blocks_structure(dataframe, db_metadata_mapping, period_resolution, True)


def __test__merge_block_structures():
    cyclone_csv_file_path = '/Users/seb/tmp/extraction_config/2000_10_cyclone_dataset.csv'
    no_cyclone_csv_file_path = '/Users/seb/tmp/extraction_config/2000_10_no_cyclone_dataset.csv'
    period_resolution = TimeResolution.MONTH

    structures = dict()
    structures['cyclone'] = __test_build_blocks_structure(cyclone_csv_file_path, period_resolution)
    structures['no_cyclone'] = __test_build_blocks_structure(no_cyclone_csv_file_path, period_resolution)

    merged_structures: List[Tuple[Period, List[Tuple[LabelId, MetaDataBlock]]]] = __merge_block_structures(structures)
    period1 = merged_structures[0][0]
    period2 = merged_structures[1][0]

    assert period1 == (2000, 9)
    assert period2 == (2000, 10)

    label_id1 = merged_structures[1][1][0][0]
    label_id2 = merged_structures[1][1][1][0]

    assert label_id1 == 'cyclone'
    assert label_id2 == 'no_cyclone'

    assert len(merged_structures[1][1][0][1]) == 49
    assert len(merged_structures[1][1][1][1]) == 51

    assert len(merged_structures[0][1]) == 1  # Cyclone not in period (2000, 9).


def __all_tests():
    __test__merge_block_structures()


if __name__ == '__main__':
    __all_tests()
