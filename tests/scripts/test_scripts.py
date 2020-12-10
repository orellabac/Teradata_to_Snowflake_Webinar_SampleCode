import os
import sys

from testutils import run

def bteq_file_BTEQ():
    res = run(['Target','scripts'], 'bteq_file_BTEQ.py')
    assert res.returncode == 0
