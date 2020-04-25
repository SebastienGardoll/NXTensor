#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 11:58:46 2019

@author: sebastien@gardoll.fr
"""
from typing import Dict, List, Mapping

from nxtensor.yaml_serializable import YamlSerializable
import logging
from nxtensor.utils.time_resolutions import TimeResolution
from abc import ABC, abstractmethod


class Variable(YamlSerializable, ABC):

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

    def __init__(self, str_id: str):
        super().__init__(str_id)
        # noinspection PyTypeChecker
        self.netcdf_attr_name: str = None
        # noinspection PyTypeChecker
        self.netcdf_path_template: str = None

    def compute_netcdf_file_path(self, time_dict: Mapping[TimeResolution, any]) -> str:
        return self.netcdf_path_template.format(**time_dict)

    def accept(self, visitor: 'VariableVisitor') -> None:
        visitor.visit_single_level_variable(self)


class MultiLevelVariable(SingleLevelVariable):

    def __init__(self, str_id: str):
        super().__init__(str_id)
        # noinspection PyTypeChecker
        self.level: int = None
        self.level_netcdf_attr_name: str = 'level'

    def accept(self, visitor: 'VariableVisitor') -> None:
        visitor.visit_multi_level_variable(self)


class ComputedVariable(Variable):

    def __init__(self, str_id: str):
        super().__init__(str_id)
        self.variable_file_paths: List[str] = list()
        self.computation_expression: str = ''   # Using Reverse Polish Notation !
        # noinspection PyTypeChecker
        self.__variables:  Dict[str, Variable] = None  # Transient for yaml serialization.

    def get_variables(self) -> Mapping[str, Variable]:
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
        self.result: Dict[str, str] = dict()
        self.time_dict: Mapping[TimeResolution,  any] = time_dict

    def visit_single_level_variable(self, variable: 'SingleLevelVariable') -> None:
        current_dict = {variable.str_id: variable.compute_netcdf_file_path(self.time_dict)}
        self.result.update(current_dict)

    def visit_multi_level_variable(self, variable: 'MultiLevelVariable') -> None:
        self.visit_single_level_variable(variable)

    def visit_computed_variable(self, variable: 'ComputedVariable') -> None:
        for variable in variable.get_variables().values():
            variable.accept(self)

    def get_result(self) -> Dict[str, str]:
        return self.result
