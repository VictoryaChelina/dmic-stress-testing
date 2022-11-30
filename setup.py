# -*- coding: utf-8 -*-

from setuptools import setup


setup(
    name="marker_generator",
    version="0.0.3",
    packages=["marker_generator"],
    include_package_data=True,
    author="Ivannikov Institute for System Programming of the RAS",
    description="",

    install_requires=[
        'easydict',
        'pyyaml'
    ],

    entry_points={
        'console_scripts': [
            'dm-decode-marker = marker_generator.decode:main',
            'dm-gen-marker = marker_generator.main:gen',
            'dm-get-marker = marker_generator.main:get',
            'dm-gen-decode = marker_generator.main:gen_decode'
        ]
    }
)
