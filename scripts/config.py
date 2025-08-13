import os

import sys

CUR_DIRNAME = os.path.dirname(__file__)
sys.path.append(os.path.join(CUR_DIRNAME, '..'))

DATA_FOLDERPATH = os.path.join(CUR_DIRNAME, '../data')
DATA_PREC_FOLDERPATH = os.path.join(DATA_FOLDERPATH, 'total_precipitation')
DATA_TEMP_FOLDERPATH = os.path.join(DATA_FOLDERPATH, '2m_temperature')
