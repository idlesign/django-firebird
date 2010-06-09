import os
from setuptools import setup, find_packages
from firebird import VERSION

f = open(os.path.join(os.path.dirname(__file__), 'README'))
readme = f.read()
f.close()

setup(
    name='django-firebird',
    version=".".join(map(str, VERSION)),
    description='Firebird backend for Django 1.2+.',
    long_description=readme,
    url='http://github.com/idlesign/django-firebird',
    packages=find_packages(),
    classifiers=[
	'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)
