"""
    Execute : python setup.py install 
    This script just install the dependencies for can execute the script main.py
"""

import os
from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    install_requires=required,
)