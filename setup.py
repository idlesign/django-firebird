from setuptools import setup, find_packages

setup(
    name = "django-firebird",
    version = "0.1",
    url = 'http://code.google.com/p/django-firebird/',
    license = 'BSD',
    description = "Firebird backend for Django.",

    packages = find_packages('firebird'),
    package_dir = {'': 'firebird'},

    install_requires = ['setuptools'],

    classifiers = [
        'Development Status :: 3 - Alpha',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP',
    ]

)
