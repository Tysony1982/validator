import os
import sys
from datetime import datetime

# Add project root to sys.path
sys.path.insert(0, os.path.abspath('..'))

project = 'Validator'
author = 'Validator'
release = '0.1'

extensions = ['docs.ext.validators']

templates_path = ['_templates']
exclude_patterns = []

html_theme = 'alabaster'
html_static_path = ['_static']
