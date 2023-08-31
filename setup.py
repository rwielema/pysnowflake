from setuptools import setup, find_packages

import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup(
    name='pysnowflake',
    version='0.0.1',
    description='Python wrapper for Snowflake',
    long_description=(here / 'README.md').read_text(encoding='utf-8'),
    long_description_content_type='text/markdown',
    author='Robbert R. Wielema',
    author_email='rwielema@gmail.com',
    url='rwielema',
    packages=find_packages(),
    install_requires=['jinja2', 'pandas', 'snowflake-connector-python', 'snowflake-connector-python[pandas]',
                      'snowflake-snowpark-python'],
    package_data={'pysnowflake': ['templates/*.sql']}
)
