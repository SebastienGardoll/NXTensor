#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 11:58:46 2019

@author: sebastien@gardoll.fr
"""

from typing import Dict, List, Mapping

from nxtensor.core.types import VariableId
from nxtensor.yaml_serializable import YamlSerializable
import logging
from nxtensor.utils.time_resolutions import TimeResolution
from abc import ABC, abstractmethod


class Variable(YamlSerializable):

    FILE_NAME_POSTFIX: str = 'variable.yml'

    def __init__(self, str_id: str):
        super().__init__(str_id)
        self.netcdf_period_resolution: TimeResolution = TimeResolution.MONTH  # Period covered by the netcdf file.
        self.time_resolution: TimeResolution = TimeResolution.HOUR  # Resolution of the time in the netcdf file.
        self.time_netcdf_attr_name: str = 'time'
        # noinspection PyTypeChecker
        self.date_template: str = None
        # noinspection PyTypeChecker
        self.lat_resolution: float = None
        # noinspection PyTypeChecker
        self.lat_nb_decimal: int = None
        self.lat_netcdf_attr_name: str = 'latitude'
        # noinspection PyTypeChecker
        self.lon_resolution: float = None
        # noinspection PyTypeChecker
        self.lon_nb_decimal: int = None
        self.lon_netcdf_attr_name: str = 'longitude'

    def compute_filename(self) -> str:
        return Variable.generate_filename(self.str_id)

    @staticmethod
    def generate_filename(str_id: str) -> str:
        return f"{str_id}_{Variable.FILE_NAME_POSTFIX}"

    @abstractmethod
    def accept(self, visitor: 'VariableVisitor') -> None:
        pass


class SingleLevelVariable(Variable):

    from datetime import datetime
    import re

    yaml_tag = u'SingleLevelVariable'

    __DATE_PATTERN = re.compile('(\d{4})_(\d{2})')
    __NOW = datetime.now()
    __now_year = int(__NOW.year)
    __now_month = int(__NOW.month)

    def __init__(self, str_id: str):
        super().__init__(str_id)
        # noinspection PyTypeChecker
        self.netcdf_attr_name: str = None
        # noinspection PyTypeChecker
        self.netcdf_path_template_periods: Dict[str, str] = None
        # Transient for yaml serialization.
        # noinspection PyTypeChecker
        self.__netcdf_path_template_mapping: Dict[int, Dict[int, str]] = None

    def compute_netcdf_file_path(self, time_dict: Mapping[TimeResolution, any]) -> str:
        netcdf_path_template_mapping_value = getattr(self, '__netcdf_path_template_mapping', None)
        if netcdf_path_template_mapping_value is None:
            self.__netcdf_path_template_mapping = \
                SingleLevelVariable.__build_pattern_mapping(self.netcdf_path_template_periods)
        netcdf_path_template = \
            self.__netcdf_path_template_mapping[time_dict[TimeResolution.YEAR]][time_dict[TimeResolution.MONTH]]
        return netcdf_path_template.format(**time_dict)

    def accept(self, visitor: 'VariableVisitor') -> None:
        visitor.visit_single_level_variable(self)

    def save(self, file_path: str) -> None:
        netcdf_path_template_mapping = self.__netcdf_path_template_mapping
        del self.__netcdf_path_template_mapping
        super().save(file_path)
        self.__netcdf_path_template_mapping = netcdf_path_template_mapping

    # Insertion order is preserved from Python version 3.6
    # Insertion order must be aligned with chronological order.
    @staticmethod
    def __build_pattern_mapping(pattern_periods: Dict[str, str]) -> Dict[int, Dict[int, str]]:
        result = dict()
        dates = list(pattern_periods.keys())
        max_index = len(dates) - 1
        for date_index in range(0, max_index+1):
            date = dates[date_index]
            match = SingleLevelVariable.__DATE_PATTERN.match(date)
            start_year = int(match.group(1))
            start_month = int(match.group(2))
            if date_index == max_index:
                end_year = SingleLevelVariable.__now_year
                end_month = SingleLevelVariable.__now_month
            else:
                end_date = dates[date_index+1]
                match = SingleLevelVariable.__DATE_PATTERN.match(end_date)
                end_year = int(match.group(1))
                end_month = int(match.group(2))
                if end_month == 1:
                    end_month = 12
                    end_year = end_year - 1
                else:
                    end_month = end_month - 1
            SingleLevelVariable.__add_pattern(result, start_year, start_month, end_year, end_month,
                                              pattern_periods[date])
        return result

    @staticmethod
    def __add_pattern(dictionary: dict, start_year: int, start_month: int, end_year: int, end_month: int, pattern: str)\
            -> None:
        for current_year in range(start_year, end_year+1):
            if current_year == start_year:
                month_down_limit = start_month
            else:
                month_down_limit = 1
            if current_year in dictionary:
                current_dict = dictionary[current_year]
            else:
                current_dict = dict()
            if current_year == end_year:
                month_up_limit = end_month
            else:
                month_up_limit = 12
            for current_month in range(month_down_limit, month_up_limit+1):
                current_dict[current_month] = pattern
            dictionary[current_year] = current_dict


class MultiLevelVariable(SingleLevelVariable):

    yaml_tag = u'MultiLevelVariable'

    def __init__(self, str_id: str):
        super().__init__(str_id)
        # noinspection PyTypeChecker
        self.level: int = None
        self.level_netcdf_attr_name: str = 'level'

    def accept(self, visitor: 'VariableVisitor') -> None:
        visitor.visit_multi_level_variable(self)


class ComputedVariable(Variable):

    yaml_tag = u'ComputedVariable'

    def __init__(self, str_id: str):
        super().__init__(str_id)
        self.variable_file_paths: List[str] = list()
        self.computation_expression: str = ''   # Using Reverse Polish Notation !
        # noinspection PyTypeChecker
        self.__variables:  Dict[str, Variable] = None  # Transient for yaml serialization.

    def get_variables(self) -> Mapping[VariableId, Variable]:
        variables_value = getattr(self, '__variables', None)
        if variables_value is None:
            logging.debug(f"loading the variables of {self.str_id}:")

            variables = list()
            for var_file_path in self.variable_file_paths:
                logging.debug(f"loading the variable {var_file_path}")
                var = Variable.load(var_file_path)
                variables.append(var)
            self.__variables = {variable.str_id: variable for variable in variables}  # Preserve the order.

        return self.__variables

    def save(self, file_path: str) -> None:
        variables = self.__variables
        del self.__variables
        super().save(file_path)
        self.__variables = variables

    def accept(self, visitor: 'VariableVisitor') -> None:
        visitor.visit_computed_variable(self)


class VariableVisitor(ABC):

    @abstractmethod
    def visit_single_level_variable(self, variable: 'SingleLevelVariable') -> None:
        pass

    @abstractmethod
    def visit_multi_level_variable(self, variable: 'MultiLevelVariable') -> None:
        pass

    @abstractmethod
    def visit_computed_variable(self, variable: 'ComputedVariable') -> None:
        pass

    @abstractmethod
    def get_result(self) -> '? object':
        pass


class VariableNetcdfFilePathVisitor(VariableVisitor):

    def __init__(self, time_dict: Mapping[TimeResolution, any]):
        self.result: Dict[VariableId, str] = dict()
        self.time_dict: Mapping[TimeResolution,  any] = time_dict

    def visit_single_level_variable(self, variable: 'SingleLevelVariable') -> None:
        current_dict = {variable.str_id: variable.compute_netcdf_file_path(self.time_dict)}
        self.result.update(current_dict)

    def visit_multi_level_variable(self, variable: 'MultiLevelVariable') -> None:
        self.visit_single_level_variable(variable)

    def visit_computed_variable(self, variable: 'ComputedVariable') -> None:
        for variable in variable.get_variables().values():
            variable.accept(self)

    def get_result(self) -> Dict[VariableId, str]:
        return self.result
