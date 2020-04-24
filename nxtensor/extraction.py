#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 13:26:39 2019

@author: sebastien@gardoll.fr
"""
from nxtensor.utils.time_utils import TimeResolution
from nxtensor.utils.db_utils import DBType, CsvOptNames, DBMetadataMapping
from nxtensor.yaml_serializable import YamlSerializable
from nxtensor.variable import Variable
import logging
from typing import List, Dict, Mapping
from enum import Enum


class ExtractionShape(Enum):

    SQUARE = 'square'


class ExtractionConfig(YamlSerializable):

    FILE_NAME_POSTFIX: str = 'extraction_config.yml'

    def compute_filename(self) -> str:
        return ExtractionConfig.generate_filename(self.str_id)

    @staticmethod
    def generate_filename(str_id: str) -> str:
        return f"{str_id}_{ExtractionConfig.FILE_NAME_POSTFIX}"

    # Because of None assignments.
    # noinspection PyTypeChecker
    def __init__(self, str_id: str):
        super().__init__(str_id)

        # Dask scheduler mode. See https://docs.dask.org/en/latest/scheduler-overview.html
        self.dask_scheduler: str = 'single-threaded'

        # x and y size of an image of the tensor.
        self.x_size: int = None
        self.y_size: int = None
        # Ordered list of variable file paths.
        self.variable_file_paths: List[str] = None
        # List of label file path descriptions.
        self.label_file_paths: List[str] = None
        self.extraction_shape: ExtractionShape = ExtractionShape.SQUARE
        # The path of required directories for an extraction.
        self.blocks_dir_path: str = None
        self.channel_dir_path: str = None
        self.tmp_dir_path: str = None

        # The maximum number of process spawn during the extraction.
        # Each process treats one extraction_metadata_blocks.
        self.nb_process: int = 1

        # The number of processes and the number of extraction_metadata_blocks should be the same so
        # as to speed up the extraction. The less the number of extraction_metadata_blocks is, the greater
        # is the size of the extraction_metadata_blocks and the longer it takes to compute it.

        self.__variables: Dict[str, Variable] = None  # Transient for yaml serialization.
        self.__labels: Dict[str, ClassificationLabel] = None  # Transient for yaml serialization.

        # TODO: save metadata options (csv).

    def save(self, file_path: str) -> None:
        variables = self.__variables
        labels = self.__labels
        del self.__variables
        del self.__labels
        super().save(file_path)
        self.__variables = variables
        self.__labels = labels

    def get_variables(self) -> Mapping[str, Variable]:
        variables_value = getattr(self, '__variables', None)
        if variables_value is None:
            logging.debug(f"loading the variables of {self.str_id}:")
            variables = list()
            for var_file_path in self.variable_file_paths:
                logging.debug(f"loading the variable {var_file_path}")
                var = Variable.load(var_file_path)
                variables.append(var)
            self.__variables: Dict[str, Variable] = {var.str_id: var for var in variables}  # Preserve the order.

        return self.__variables

    def get_labels(self) -> Mapping[str, 'ClassificationLabel']:
        labels_value = getattr(self, '__labels', None)
        if labels_value is None:
            logging.debug(f"loading the labels of {self.str_id}:")

            labels: ['ClassificationLabel'] = list()
            for label_file_path in self.label_file_paths:
                logging.debug(f"loading the label {label_file_path}")
                label = ClassificationLabel.load(label_file_path)
                labels.append(label)
            self.__labels = {label.str_id: label for label in labels}  # Preserve the order.

        return self.__labels


class ClassificationLabel(YamlSerializable):

    FILE_NAME_POSTFIX: str = 'label.yml'

    def compute_filename(self) -> str:
        return ClassificationLabel.generate_filename(self.dataset_id, self.str_id)

    @staticmethod
    def generate_filename(dataset_id: str, display_name: str) -> str:
        return f"{dataset_id}_{display_name}_{ClassificationLabel.FILE_NAME_POSTFIX}"

    # Because of None assignments.
    # noinspection PyTypeChecker
    def __init__(self, str_id: str, dataset_id: str):
        super().__init__(str_id)

        # Numerical id that encode the label.
        self.num_id: float = None
        # The dataset identifier.
        self.dataset_id: str = dataset_id
        # The path to the db that contains the information of the labels.
        self.db_file_path: str = None
        # The format of the data base of labels (CSV, etc.).
        self.db_format: DBType = None

        # The description of the db options (dictionary).
        self.db_format_options: Dict[CsvOptNames, str] = None

        # Dictionary that maps required information about the labels:
        # convert keys (see enum_utils) into db column names.
        # noinspection PyTypeChecker
        self.db_meta_data_mapping: DBMetadataMapping = None

        # Time resolution in the db.
        self.db_time_resolution: TimeResolution = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(str_id={self.str_id}, dataset_id={self.dataset_id}, " + \
               f"num_id={self.num_id}, db={self.db_file_path})"
