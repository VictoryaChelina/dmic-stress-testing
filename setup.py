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
        'tqdm'

    ],
    entry_points={
         'console_scripts': [
            'make-stress = dmic_stress_testing.common:main'
        ]
    }
)
