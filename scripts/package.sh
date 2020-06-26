#!/bin/bash

set -e

rm -fr build dist NXTensor.egg-info

# Requires wheel setuptools twine
# conda install wheel setuptools twine

# Regenerate distribution packages
python setup.py sdist bdist_wheel

# Testing purpose:
# python3 -m twine upload --repository testpypi dist/*
# python3 -m pip install --index-url https://test.pypi.org/simple/ nxtensor
python -m twine upload dist/*

rm -fr build dist NXTensor.egg-info
