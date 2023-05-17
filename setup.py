# -*- coding: utf-8 -*-

from setuptools import setup


setup(
    name="dmic-stress-testing",
    version="0",
    packages=["dmic_stress_testing"],
    include_package_data=True,
    author="Ivannikov Institute for System Programming of the RAS",
    description="Stress tessting of DMIC server",

    install_requires=[
        'infi.clickhouse-orm',
        'numpy',
        'aiochclient',
        'aiohttp',
        'tqdm',
        'requests_toolbelt'

    ],
    entry_points={
         'console_scripts': [
            'make-stress = dmic_stress_testing.main:main',
            'check = dmic_stress_testing.checker:check',
            'partitions = dmic_stress_testing.partitions_log:main',
            'create-tables = dmic_stress_testing.creating_tables:main'
        ]
    }
)
