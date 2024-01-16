from setuptools import setup, find_packages
import os

VERSION = '2.7.8'
DESCRIPTION = 'zdt'

# Setting up
setup(
    name="zdt",
    version=VERSION,
    author="Dickson",
    author_email="<suwon2912@gmail.com>",
    description=DESCRIPTION,
    packages=find_packages(),
    install_requires=['sqlalchemy', 'typing', 'hdbcli','redmail','pandas>=1.1','datetime','python-dateutil','numpy','apache-airflow','azure-storage-file-datalake'],
    keywords=['python'],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)






