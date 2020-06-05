#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 15:29:43 2019

@author: sebastien@gardoll.fr
"""

import re
import logging
from typing import Mapping

import dask
import numpy as np
import xarray as xr


# This class implement RPN calculator for Xarray's DataArray and scalars.
from nxtensor.core.types import VariableId


class XarrayRpnCalculator:

    @staticmethod
    # Return left_operand + right_operand
    def __addition(left_operand, right_operand):
        return (left_operand + right_operand).compute()

    @staticmethod
    # Return left_operand - right_operand
    def __subtraction(left_operand, right_operand):
        return (left_operand - right_operand).compute()

    @staticmethod
    # Return left_operand *
    def __multiplication(left_operand, right_operand):
        return (left_operand * right_operand).compute()

    @staticmethod
    # Return left_operand / right_operand
    def __division(left_operand, right_operand):
        return (left_operand / right_operand).compute()

    @staticmethod
    # Return the square root of the operand (√x)
    # Compliant only with numpy > 1.13
    def __sqrt(operand):
        result = np.sqrt(operand)
        return xr.DataArray(data=result)

    @staticmethod
    # Return the square of the operand (x²)
    # Compliant only with numpy > 1.13
    def __square(operand):
        result = np.square(operand)
        return xr.DataArray(data=result)

    @staticmethod
    # Return the log base 10 of the operand
    # Compliant only with numpy > 1.13
    def __log10(operand):
        result = np.log10(operand)
        return xr.DataArray(data=result)

    @staticmethod
    # Return the raise of left_operand to the power of right_operand
    def __power(left_operand, right_operand):
        result = np.power(left_operand, right_operand)
        return xr.DataArray(data=result)

    TOKENIZER = re.compile(r'\s+')

    # Adding .__func__ is mandatory otherwise: ''staticmethod' object is not callable'.
    # Arity, static method.
    OPERATORS = {'+'      : (2, __addition.__func__),
                 '-'      : (2, __subtraction.__func__),
                 '*'      : (2, __multiplication.__func__),
                 '/'      : (2, __division.__func__),
                 'log10'  : (1, __log10.__func__),
                 'square' : (1, __square.__func__),  # x²
                 'sqrt'   : (1, __sqrt.__func__),   # √x
                 'pow'    : (2, __power.__func__)}

    def __init__(self, expression: str, extracted_regions: Mapping[VariableId, xr.DataArray], dask_scheduler: str):
        self.__expression = expression
        self.__stack = list()
        self.__intermediate_results = dict()
        self.__extracted_regions = extracted_regions
        self.__dask_scheduler = dask_scheduler
        # noinspection PyTypeChecker
        self.__result: xr.DataArray = None

    def __check_tokens(self, tokens):
        for index in range(0, len(tokens)):
            token = tokens[index]
            if token:
                if token not in XarrayRpnCalculator.OPERATORS and token not in self.__extracted_regions:
                    try:
                        logging.debug(f"trying to convert '{token}' into a float at index {index}")
                        new_token = float(token)
                        tokens[index] = new_token
                    except Exception:
                        msg = f"unexpected token '{token}'"
                        raise Exception(msg)
            else:
                tokens.pop(index)  # Ignore empty strings.

        return tokens

    def get_result(self) -> xr.DataArray:
        if self.__result is None:
            result_id = self.__stack[0]
            self.__result = self.__intermediate_results.get(result_id, None)

        if self.__result is None:
            raise Exception("missing result")
        else:
            return self.__result

    # Return a xarray's DataArray instance from the given mapping
    # (self.data_array_mapping) or intermediate_results.
    # Otherwise return the literal as it is a scalar (i.e. a float).
    def __resolve_operand(self, operand_literal):
        if operand_literal in self.__intermediate_results:
            logging.debug(f"resolve '{operand_literal}' as an intermediate result")
            return self.__intermediate_results[operand_literal]
        else:
            if operand_literal in self.__extracted_regions:
                logging.debug(f"resolve '{operand_literal}' as a input array")
                return self.__extracted_regions[operand_literal]
            else:
                logging.debug(f"resolve '{operand_literal}' as a constant")
                return operand_literal

    def __compute(self):
        operator_literal = self.__stack.pop()
        nb_operand, operation = XarrayRpnCalculator.OPERATORS[operator_literal]
        logging.debug(f"pop operator '{operator_literal}' with arity of '{nb_operand}'")

        if nb_operand == 1:
            operand_literal = self.__stack.pop()
            # Convert the label into string so as to avoid confusion with scalar
            # (float) as hash function return an integer value.
            raw_label = f"{operator_literal}#{operand_literal}"
            label = str(hash(raw_label))
            if label in self.__intermediate_results:
                logging.debug(f"getting already computed intermediate result for label '{raw_label}'")
                intermediate_result = self.__intermediate_results[label]
            else:
                logging.debug(f"resolving operand_literal '{operand_literal}'")
                resolved_operand = self.__resolve_operand(operand_literal)
                logging.debug(f"computing intermediate with label '{raw_label}', operator '{operator_literal}' " +
                              f"and operand '{operand_literal}'")
                intermediate_result = operation(resolved_operand)
        else:
            right_operand_literal = self.__stack.pop()
            left_operand_literal  = self.__stack.pop()
            # Convert the label into string so as to avoid confusion with scalar
            # (float) as hash function return an integer value.
            raw_label = f"{left_operand_literal}#{operator_literal}#{right_operand_literal}"
            label = str(hash(raw_label))
            if label in self.__intermediate_results:
                logging.debug(f"getting already computed intermediate result for label '{raw_label}'")
                intermediate_result = self.__intermediate_results[label]
            else:
                logging.debug(f"resolving operand_literal '{right_operand_literal}'")
                right_resolved_operand = self.__resolve_operand(right_operand_literal)
                logging.debug(f"resolving operand_literal '{left_operand_literal}'")
                left_resolved_operand  = self.__resolve_operand(left_operand_literal)
                logging.debug(f"computing intermediate with label '{raw_label}', operator '{operator_literal}', " +
                              f"left operand '{left_operand_literal}' and right operand '{right_operand_literal}'")
                intermediate_result    = operation(left_resolved_operand, right_resolved_operand)

        logging.debug(f"staking result with a shape of {intermediate_result.shape} and label '{raw_label}'")
        self.__stack.append(label)
        self.__intermediate_results[label] = intermediate_result

    def compute(self):
        tokens = XarrayRpnCalculator.TOKENIZER.split(self.__expression)
        tokens = self.__check_tokens(tokens)
        logging.debug(f"computing tokens: {tokens}")
        for token in tokens:
            logging.debug(f"appending token '{token}' on the stack")
            self.__stack.append(token)
            if token in XarrayRpnCalculator.OPERATORS:
                with dask.config.set(scheduler=self.__dask_scheduler):
                    self.__compute()
        return self.get_result()
