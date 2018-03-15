'''
Created on Mar 15, 2018
@author: hasitha
'''

from airflow.operators.sensors import BaseSensorOperator
from subprocess import Popen, PIPE


class RunHecDssSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super(RunHecDssSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        process = Popen(['./test.sh'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        return_code = process.returncode
        print return_code
