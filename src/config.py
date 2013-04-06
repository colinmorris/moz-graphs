__author__ = 'colin'

import os

# TODO: This can probably be done portably using __module__ and relative paths.
ROOT_DIR = os.path.join(os.getenv('HOME'), 'work/moz-graphs')

DATA_DIR = os.path.join(ROOT_DIR, 'data')
LOG_DIR = os.path.join(ROOT_DIR, 'logs')
ADJ_DIR = os.path.join(ROOT_DIR, 'adj')

DB_PATH = os.path.join(ROOT_DIR, 'src', 'alch.db')

DB_URI = 'sqlite:///' + DB_PATH


# TODO: Have a variable holding a list of names of modules that contain mapped classes?
# There are a few cases where I want to import all such modules (for the sake of an
# omniscient declarative base), and I don't want to repeat myself...
