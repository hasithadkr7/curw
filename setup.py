from distutils.core import setup

setup(
    name='curw',
    version='1.0.0',
    packages=['model', 'model.hec_hms', 'model.hec_hms.operators', 'model.hec_hms.sensors', 'model.hec_hms.utils',
              'model.workflow', 'model.workflow.airflow', 'model.workflow.airflow.dags'],
    url='',
    license='Apache 2.0',
    author='hasitha dhananjaya',
    author_email='hasitha.10@cse.mrt.ac.lk',
    description='',
    requires=['airflow', 'pyyaml', 'shapely','ordereddict']
)