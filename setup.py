# -*- coding: utf-8 -*-

from setuptools import setup


setup(
    name="many_users_work",
    version="0",
    packages=["dmic-stress-testing"],
    include_package_data=True,
    author="Ivannikov Institute for System Programming of the RAS",
    description="Stress tessting of DMIC server",

    install_requires=[
        'marker_generator',
    ],

    # entry_points={
    #     'console_scripts': [
    #         'dm-decode-marker = marker_generator.decode:main',
    #         'dm-gen-marker = marker_generator.main:gen',
    #         'dm-get-marker = marker_generator.main:get',
    #         'dm-gen-decode = marker_generator.main:gen_decode'
    #     ]
    # }
)
