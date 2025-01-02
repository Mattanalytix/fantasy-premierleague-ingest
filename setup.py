from setuptools import setup, find_packages
from os import path


working_directory = path.abspath(path.dirname(__file__))

with open(path.join(working_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='fpl_connector',
    version='0.0.1',
    url='https://europe-west2-python.pkg.dev/raw-prod-service-k41/fpl-connector/',
    author='mattanalytix',
    author_email='info@mattanalytix.com',
    description='A connector application for the fantasy.premierleague api',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    include_package_data=True,
    package_data={'fpl_connector': ['templates/*']},
    install_requires=[
        'bigquery-etl-tools>=0.0.3',
        'pyyaml>=6.0.0'
    ],
)
