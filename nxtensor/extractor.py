#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 23 14:50:20 2020

@author: sebastien@gardoll.fr
"""
from typing import Dict, List, Mapping, Union, Sequence

from nxtensor.core.square_extractor import SquareRegionExtractionVisitor
from nxtensor.core.xarray_channel_extraction import Block, Period
from nxtensor.utils.coordinate_utils import Coordinate
from nxtensor.utils.tensor_utils import TensorDimension
from nxtensor.extraction import ExtractionShape
from nxtensor.variable import VariableVisitor, SingleLevelVariable, MultiLevelVariable, ComputedVariable, Variable, \
    VariableNetcdfFilePathVisitor

import nxtensor.core.xarray_extractions as xtract
import nxtensor.utils.time_utils as tu

import nxtensor.core.xarray_channel_extraction as chan_xtract

import xarray as xr


class ExtractionVisitor(VariableVisitor):

    # TODO: instantiate the visitor thanks to a factory (shape).
    def __init__(self, period: Period, extraction_metadata_blocks: Mapping[str, Block], half_lat_frame: int,
                 half_lon_frame: int, dask_scheduler: str = 'single-threaded',
                 shape: ExtractionShape = ExtractionShape.SQUARE):
        self.__period: Period = period
        self.__extraction_metadata_blocks: Mapping[str, Block] = extraction_metadata_blocks
        self.__half_lat_frame: int = half_lat_frame
        self.__half_lon_frame: int = half_lon_frame
        self.__dask_scheduler: str = dask_scheduler
        self.__shape: ExtractionShape = shape
        self.result: Dict[str, xr.DataArray] = dict()

    def __core_extraction(self, var: Variable, datasets: Mapping[str, xr.Dataset]) -> None:

        for label_id, extraction_metadata_block in self.__extraction_metadata_blocks.items():
            # noinspection PyTypeChecker
            extraction_data_list: Sequence[Mapping[Union[Coordinate, tu.TimeResolution], Union[int, float]]] = \
                chan_xtract.convert_block_to_dict(extraction_metadata_block)

            extracted_regions: List[xr.DataArray] = list()
            # The order of extraction_data_list must be deterministic so as all the channel
            # match their extracted region line by line.
            for extraction_data in extraction_data_list:
                # TODO: instantiate the visitor thanks to a factory (shape).
                extractor: SquareRegionExtractionVisitor = \
                    SquareRegionExtractionVisitor(datasets=datasets,
                                                  extraction_data=extraction_data,
                                                  half_lat_frame=self.__half_lat_frame,
                                                  half_lon_frame=self.__half_lon_frame,
                                                  dask_scheduler=self.__dask_scheduler)
                var.accept(extractor)
                extracted_regions.append(extractor.get_result())

            # dims are lost when instantiating a DataArray based on other DataArray objects.
            dims = (var.str_id, TensorDimension.X, TensorDimension.Y)
            # Stack the extracted regions in a xarray data array => data extraction_metadata_block.
            data = xr.DataArray(extracted_regions, dims=dims)
            self.result[label_id] = data

        [dataset.close() for dataset in datasets.values()]

    def visit_single_level_variable(self, var: SingleLevelVariable) -> None:
        time_dict = tu.from_time_list_to_dict(self.__period)
        netcdf_file_path = var.compute_netcdf_file_path(time_dict)
        datasets = {var.str_id: xtract.open_netcdf(netcdf_file_path)}
        self.__core_extraction(var, datasets)

    def visit_multi_level_variable(self, var: MultiLevelVariable) -> None:
        self.visit_single_level_variable(var)

    def visit_computed_variable(self, var: ComputedVariable) -> None:
        time_dict = tu.from_time_list_to_dict(self.__period)
        visitor = VariableNetcdfFilePathVisitor(time_dict)
        datasets: Dict[str, xr.Dataset] = dict()
        for var_id, netcdf_file_path in visitor.result:
            datasets[var_id] = xtract.open_netcdf(netcdf_file_path)
        self.__core_extraction(var, datasets)

    def get_result(self) -> Dict[str, xr.DataArray]:
        return self.result
