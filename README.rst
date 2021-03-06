django-firebird
===============
http://github.com/idlesign/django-firebird

*Firebird backend for Django 1.2+*

Original *django-firebird* could be found at http://code.google.com/p/django-firebird/

This project is a Github fork from the original django-firebird.

Requirements
------------

* Django 1.2+
* KInterbasDB 3.3+

Strongly advised:

* Firebird 2+


Usage
-----

1. Define your Firebird database in DATABASES dictionary in your Django settings file (settings.py)::

    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'testdb',
            'USER': 'testdb_user',
            'PASSWORD': 'testdbpassw',
            'HOST': '',
            'PORT': '',
        },
        'fb': {
            'ENGINE': 'firebird',
            'NAME': '/home/idle/mydb.gdb',
            'USER': 'SYSDBA',
            'PASSWORD': 'passw',
            'HOST': '127.0.0.1',
            'PORT': '3050',
            'OPTIONS': {'charset':'WIN1251', 'dialect':1} ,
        }
    }

  Above we defined two databases: 

  1) `default` – default PostrgeSQL database for our application and 

  2) `fb` some additional Firebird database.

  **ENGINE** part tells Django to use django-firebird package.  

  Note, that we do not put `firebird` folder from django-firebird package into `$your_django_path$/db/backends/` so we use 'firebird' instead of 'django.db.backends.firebird'.

  **NAME** part contains database file path.

  **OPTIONS** part here contains optional db parameters. 
  
  `dialect` parameter restricts sql dialect:
  
    **Dialect 1**: table and field names are capitalized, no double quotes, date field type contains date and time.
  
    **Dialect 3**: table and field names are double quoted, case unchanged, date field type contains only date.

2. Create your models.py as usual. 
3. While fetching data call **using()** method::

    mySubjects = Subject.objects.all().using('fb')
    # Here we use `Subject` class defined in our models.py and fetch all data we need using ‘fb’ database.


How to
------

1. `What if some table in legacy database has no Primary Key?`

  Firebird allows to check for internal table’s ‘RDB$DB_KEY’ field.

  In a model you can define this field as follows::

    DB_KEY = models.CharField(primary_key=True, db_column='RDB$DB_KEY', max_length=10)

2. `What if you want to pass some params to KInterbasDB init() method?`

  Define params in database configuration OPTIONS dictionary 'init_params' item like this::

    'fb': {
        'ENGINE': 'firebird',
        ...
        'OPTIONS': {'init_params': {'type_conv':0}},  # This sets type_conv option to 0. Insane am I? %)
        ...
    }

