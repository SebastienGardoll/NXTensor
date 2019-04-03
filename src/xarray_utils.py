#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 15:29:43 2019

@author: sebastien@gardoll.fr
"""

import re
import logging
import dask
import numpy as np
import xarray as xr

DEFAULT_DASK_SCHEDULER = 'single-threaded'

# This class implement RPN calculator for Xarray's DataArray and scalars.
class XarrayRpnCalculator:

  @staticmethod
  # Return left_operand + right_operand
  def addition(left_operand, right_operand):
    return (left_operand + right_operand).compute()

  @staticmethod
  # Return left_operand - right_operand
  def subtraction(left_operand, right_operand):
    return (left_operand - right_operand).compute()

  @staticmethod
  # Return left_operand *
  def multiplication(left_operand, right_operand):
    return (left_operand * right_operand).compute()

  @staticmethod
  # Return left_operand / right_operand
  def division(left_operand, right_operand):
    return (left_operand / right_operand).compute()

  @staticmethod
  # Return the square root of operand
  # Compliant only with numpy > 1.13
  def square(operand):
    result = np.square(operand)
    return xr.DataArray(data=result)

  @staticmethod
  # Return the log base 10 of operand
  # Compliant only with numpy > 1.13
  def log10(operand):
    result = np.log10(operand)
    return xr.DataArray(data=result)

  @staticmethod
  # Return the raise of left_operand to the power of right_operand
  def power(left_operand, right_operand):
    result = np.power(left_operand, right_operand)
    return xr.DataArray(data=result)

  TOKENIZER = re.compile(r'\s+')

  # Adding .__func__ is mandatory otherwise: ''staticmethod' object is not callable'.
                     # Arity, static method.
  OPERATORS = {'+'    : (2, addition.__func__),
               '-'    : (2, subtraction.__func__),
               '*'    : (2, multiplication.__func__),
               '/'    : (2, division.__func__),
               'log10': (1, log10.__func__),
               'sqrt' : (1, square.__func__),
               'pow'  : (2, power.__func__)}

  def __init__(self, expression, data_array_mapping):#, dask_scheduler, nb_workers):
    self._expression = expression
    self._stack = list()
    self._intermediate_results = dict()
    self._data_array_mapping = data_array_mapping
    self.dask_scheduler = DEFAULT_DASK_SCHEDULER

  def _check_tokens(self, tokens):
    for index in range(0, len(tokens)):
      token = tokens[index]
      if token:
        if not token in XarrayRpnCalculator.OPERATORS and\
           not token in self._data_array_mapping:
          try:
            logging.debug(f"trying to convert '{token}' into a float at index {index}")
            new_token = float(token)
            tokens[index] = new_token
          except:
            msg = f"unexpected token '{token}'"
            logging.error(msg)
            raise Exception(msg)
      else:
        tokens.pop(index) # Ignore empty strings.

    return tokens

  def get_result(self):
    result_id = self._stack[0]
    result = self._intermediate_results.get(result_id, None)
    if result is None:
      raise Exception("missing result")
    else: return result

  # Return a xarray's DataArray instance from the given mapping
  # (self.data_array_mapping) or intermediate_results.
  # Otherwise return the literal as it is a scalar (i.e. a float).
  def _resolve_operand(self, operand_literal):
    if operand_literal in self._intermediate_results:
      return self._intermediate_results[operand_literal]
    else:
      return self._data_array_mapping.get(operand_literal, operand_literal)

  def _compute(self):
    operator = self._stack.pop()
    nb_operand, operation = XarrayRpnCalculator.OPERATORS[operator]
    logging.debug(f"computing operator '{operator}' with arity of '{nb_operand}'")

    if nb_operand == 1:
      operand_literal = self._stack.pop()
      # Convert the label into string so as to avoid confusion with scalar
      # (float) as hash function return an integer value.
      label = str(hash(f"{operator}#{operand_literal}"))
      if label in self._intermediate_results:
        logging.debug(f"getting already computed intermediate result for label '{label}'")
        intermediate_result = self._intermediate_results[label]
      else:
        logging.debug(f"resolving operand_literal '{operand_literal}'")
        resolved_operand = self._resolve_operand(operand_literal)
        logging.debug(f"computing intermediate with label '{label}' and operand '{resolved_operand}'")
        intermediate_result = operation(resolved_operand)
    else:
      right_operand_literal = self._stack.pop()
      left_operand_literal  = self._stack.pop()
      # Convert the label into string so as to avoid confusion with scalar
      # (float) as hash function return an integer value.
      label = str(hash(f"{left_operand_literal}#{operator}#{right_operand_literal}"))
      if label in self._intermediate_results:
        logging.debug(f"getting already computed intermediate result for label '{label}'")
        intermediate_result = self._intermediate_results[label]
      else:
        logging.debug(f"resolving operand_literal '{right_operand_literal}'")
        right_resolved_operand = self._resolve_operand(right_operand_literal)
        logging.debug(f"resolving operand_literal '{left_operand_literal}'")
        left_resolved_operand  = self._resolve_operand(left_operand_literal)
        logging.debug(f"computing intermediate with label '{label}', left operand '{left_resolved_operand}' and right operand '{right_resolved_operand}'")
        intermediate_result    = operation(left_resolved_operand, right_resolved_operand)

    logging.debug(f"staking result with a shape of {intermediate_result.shape} and label {label}")
    self._stack.append(label)
    self._intermediate_results[label] = intermediate_result

  def compute(self):
    tokens = XarrayRpnCalculator.TOKENIZER.split(self._expression)
    tokens = self._check_tokens(tokens)
    logging.debug(f"computing tokens: {tokens}")
    for token in tokens:
      logging.debug(f"appending token '{token}' on the stack")
      self._stack.append(token)
      if token in XarrayRpnCalculator.OPERATORS:
        with dask.config.set(scheduler=self.dask_scheduler):
          self._compute()
    return self.get_result()
