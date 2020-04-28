#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 23 14:50:20 2020

@author: sebastien@gardoll.fr
"""
from typing import Dict, List, Mapping, Tuple

from nxtensor.square_extractor import SquareRegionExtractionVisitor
from nxtensor.core.xarray_channel_extraction import MetaDataBlock, Period
from nxtensor.utils.tensor_dimensions import TensorDimension
from nxtensor.extraction import ExtractionShape
from nxtensor.variable import VariableVisitor, SingleLevelVariable, MultiLevelVariable, ComputedVariable, Variable, \
    VariableNetcdfFilePathVisitor, VariableId

import nxtensor.core.xarray_extractions as xtract
import nxtensor.utils.time_utils as tu

from nxtensor.core.xarray_channel_extraction import LabelId

import xarray as xr


class ExtractionVisitor(VariableVisitor):

    # TODO: instantiate the visitor thanks to a factory (shape).
    def __init__(self, period: Period, extraction_metadata_blocks: List[Tuple[LabelId, MetaDataBlock]],
                 half_lat_frame: int,
                 half_lon_frame: int, dask_scheduler: str = 'single-threaded',
                 shape: ExtractionShape = ExtractionShape.SQUARE):
        self.__period: Period = period
        self.__extraction_metadata_blocks: List[Tuple[LabelId, MetaDataBlock]] = extraction_metadata_blocks
        self.__half_lat_frame: int = half_lat_frame
        self.__half_lon_frame: int = half_lon_frame
        self.__dask_scheduler: str = dask_scheduler
        self.__shape: ExtractionShape = shape
        self.result: List[Tuple[LabelId, xr.DataArray, MetaDataBlock]] = list()

    def __core_extraction(self, var: Variable, datasets: Mapping[VariableId, xr.Dataset]) -> None:

        for label_id, extraction_metadata_block in self.__extraction_metadata_blocks:
            extracted_regions: List[xr.DataArray] = list()
            # The order of extraction_data_list must be deterministic so as all the channel
            # match their extracted region line by line.
            for extraction_data in extraction_metadata_block:
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
            # Stack the extracted regions in a xarray data array => data extraction_metadata_blocks.
            data = xr.DataArray(extracted_regions, dims=dims)
            self.result.append((label_id, data, extraction_metadata_block))

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
        var.accept(visitor)
        datasets: Dict[VariableId, xr.Dataset] = dict()
        for var_id, netcdf_file_path in visitor.result.items():
            datasets[var_id] = xtract.open_netcdf(netcdf_file_path)
        self.__core_extraction(var, datasets)

    def get_result(self) -> List[Tuple[LabelId, xr.DataArray, MetaDataBlock]]:
        return self.result
