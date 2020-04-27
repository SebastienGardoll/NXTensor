#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  22 11:14:54 2020

@author: sebastien@gardoll.fr
"""
from typing import Dict, Tuple, List

from nxtensor.core.xarray_channel_extraction import MetaDataBlock, Period
from nxtensor.extraction import ExtractionConfig
from nxtensor.extractor import ExtractionVisitor
from nxtensor.utils.db_utils import DBMetadataMapping
from nxtensor.variable import Variable

import nxtensor.core.xarray_channel_extraction as chan_xtract
from nxtensor.core.xarray_channel_extraction import LabelId

import pandas as pd

import xarray as xr

import os.path as path

import nxtensor.utils.db_utils as du


# Single process - single thread.
def preprocess_extraction(preprocess_result_file_path: str, extraction_conf_file_path: str) -> None:
    # TODO: error handling. Factorize !
    extraction_conf = ExtractionConfig.load(extraction_conf_file_path)
    db_metadata_mappings: Dict[LabelId, DBMetadataMapping] = dict()
    extraction_metadata_blocks: Dict[LabelId, pd.DataFrame] = dict()
    for label_id, label in extraction_conf.get_labels().items():
        db_metadata_mappings[label_id] = label.db_meta_data_mapping
        dataframe_load_function = du.get_dataframe_load_function(label.db_format)
        extraction_metadata_block = dataframe_load_function(label.db_file_path, label.db_format_options)
        extraction_metadata_blocks[label_id] = extraction_metadata_block

    netcdf_period_resolution = None
    for variable in extraction_conf.get_variables().values():
        netcdf_period_resolution = variable.netcdf_period_resolution
        break

    chan_xtract.preprocess_extraction(preprocess_result_file_path=preprocess_result_file_path,
                                      extraction_metadata_blocks=extraction_metadata_blocks,
                                      db_metadata_mappings=db_metadata_mappings,
                                      netcdf_file_time_period=netcdf_period_resolution,
                                      inplace=True)


def extract(preprocess_result_file_path: str,
            extraction_conf_file_path: ExtractionConfig, variable_id: str)\
            -> Dict[Period, Dict[str, Dict[str, str]]]:

    # TODO: error handling. Factorize !
    extraction_conf = ExtractionConfig.load(extraction_conf_file_path)
    variable: Variable = extraction_conf.get_variables()[variable_id]
    file_prefix_path = path.join(extraction_conf.blocks_dir_path, extraction_conf.str_id)

    def process_block(period: Period, extraction_metadata_blocks: List[Tuple[LabelId, MetaDataBlock]]) \
            -> Tuple[str, List[Tuple[LabelId, xr.DataArray, MetaDataBlock]]]:
        # Must be a integer !!! TODO: check for that when designing an extraction.
        half_lat_frame = int((extraction_conf.y_size * variable.lat_resolution)/2)
        half_lon_frame = int((extraction_conf.y_size * variable.lat_resolution)/2)
        extractor: ExtractionVisitor = ExtractionVisitor(period=period,
                                                         extraction_metadata_blocks=extraction_metadata_blocks,
                                                         half_lat_frame=half_lat_frame,
                                                         half_lon_frame=half_lon_frame,
                                                         dask_scheduler=extraction_conf.dask_scheduler,
                                                         shape=extraction_conf.extraction_shape)
        variable.accept(extractor)
        result: Tuple[str, List[Tuple[LabelId, xr.DataArray, MetaDataBlock]]] = \
            (file_prefix_path, extractor.get_result())
        return result

        # TODO: save metadata options (csv).
    file_paths = chan_xtract.extract(preprocess_result_file_path=preprocess_result_file_path,
                                     extraction_metadata_block_processing_function=process_block,
                                     nb_workers=extraction_conf.nb_process)
    return file_paths


def test_preprocess_extraction(preprocess_result_file_path: str, extraction_conf_file_path: str) -> None:
    preprocess_extraction(preprocess_result_file_path, extraction_conf_file_path)


def test_extract(preprocess_result_file_path: str, extraction_conf_file_path: str,
                 variable_id: str) -> None:
    extract(preprocess_result_file_path, extraction_conf_file_path, variable_id)


if __name__ == '__main__':
    config_dir_path = '/home/sgardoll/extraction_config'
    preprocess_result_file_path = path.join(config_dir_path, 'preprocessing.pkl')
    extraction_conf_file_path = path.join(config_dir_path, '2000_10_extraction_config.yml')

    test_preprocess_extraction(preprocess_result_file_path, extraction_conf_file_path)

    variable_id = 'msl'
    test_extract(preprocess_result_file_path, extraction_conf_file_path, variable_id)
