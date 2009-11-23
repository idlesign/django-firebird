"""
Firebird database backend for Django.

Requires kinterbasdb: http://www.firebirdsql.org/index.php?op=devel&sub=python
"""

import os
import datetime
import time
try:
    from decimal import Decimal
except ImportError:
    from django.utils._decimal import Decimal

try:
    import kinterbasdb as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading kinterbasdb module: %s" % e)



from django.db.backends import *
from django.db.backends.firebird import query
from django.db.backends.firebird.client import DatabaseClient
from django.db.backends.firebird.creation import DatabaseCreation
from django.db.backends.firebird.introspection import DatabaseIntrospection

#from django.utils.encoding import smart_str, smart_unicode, force_unicode


DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
OperationalError = Database.OperationalError

class DatabaseFeatures(BaseDatabaseFeatures):
    uses_custom_query_class = True

class DatabaseOperations(BaseDatabaseOperations):
    """
    This class encapsulates all backend-specific differences, such as the way
    a backend performs ordering or calculates the ID of a recently-inserted
    row.
    """

    def __init__(self):
        self._engine_version = None
    
    def _get_engine_version(self):
        if self._engine_version is None:
            from django.db import connection            
            self._engine_version = connection.get_server_version()
        return self._engine_version
    engine_version = property(_get_engine_version)
    
    def _get_firebird_version(self):
        return [int(val) for val in self.engine_version.split()[-1].split('.')]
    firebird_version = property(_get_firebird_version)

    def autoinc_sql(self, table, column):
        # To simulate auto-incrementing primary keys in Firebird, we have to create a generator and a trigger.
        gn_name = self.quote_name(self.get_generator_name(table))
        tr_name = self.quote_name(self.get_trigger_name(table))
        tbl_name = self.quote_name(table)
        col_name = self.quote_name(column)
        generator_sql = """CREATE GENERATOR %(gn_name)s""" % locals()
        trigger_sql = """
            CREATE TRIGGER %(tr_name)s FOR %(tbl_name)s
            BEFORE INSERT
            AS 
            BEGIN
               IF (NEW.%(col_name)s IS NULL) THEN 
                   NEW.%(col_name)s = GEN_ID(%(gn_name)s, 1);
            END""" % locals()
        return generator_sql, trigger_sql

    def date_extract_sql(self, lookup_type, field_name):
        # Firebird uses WEEKDAY keyword.
        lkp_type = lookup_type
        if lkp_type == 'week_day':
            lkp_type = 'weekday'            
        return "EXTRACT(%s FROM %s)" % (lkp_type.upper(), field_name)

    def date_trunc_sql(self, lookup_type, field_name):
        if lookup_type == 'year':
             sql = "EXTRACT(year FROM %s)||'-01-01 00:00:00'" % field_name
        elif lookup_type == 'month':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-01 00:00:00'" % (field_name, field_name)
        elif lookup_type == 'day':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-'||EXTRACT(day FROM %s)||' 00:00:00'" % (field_name, field_name, field_name)
        return "CAST(%s AS TIMESTAMP)" % sql

    def last_insert_id(self, cursor, table_name, pk_name):
        cursor.execute('SELECT GEN_ID(%s, 0) FROM rdb$database' % (self.get_generator_name(table_name),))
        return cursor.fetchone()[0]

    def max_name_length(self):
        return 31

    def convert_values(self, value, field):
        return super(DatabaseOperations, self).convert_values(value, field)

    def query_class(self, DefaultQueryClass):  
        return query.query_class(DefaultQueryClass)
            
    def quote_name(self, name):
        if not name.startswith('"') and not name.endswith('"'):
            name = '"%s"' % util.truncate_name(name, self.max_name_length())
        return name.upper()

    def get_generator_name(self, table_name):
        return '%s_GN' % util.truncate_name(table_name, self.max_name_length() - 3).upper()

    def get_trigger_name(self, table_name):
        name_length = DatabaseOperations().max_name_length() - 3
        return '%s_TR' % util.truncate_name(table_name, self.max_name_length() - 3).upper()
    

class DatabaseWrapper(BaseDatabaseWrapper):
    """
    Represents a database connection.
    
    Inherited from BaseDatabaseWrapper:
     self.connection = None
     self.queries = []
     self.settings_dict = settings_dict
    """
    
    import kinterbasdb.typeconv_datetime_stdlib as tc_dt
    import kinterbasdb.typeconv_fixed_decimal as tc_fd
    import kinterbasdb.typeconv_text_unicode as tc_tu
    import django.utils.encoding as dj_ue

    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': "LIKE %s ESCAPE'\\'",
        'icontains': 'CONTAINING %s', #case is ignored
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'STARTING WITH %s', #looks to be faster then LIKE
        'endswith': "LIKE %s ESCAPE'\\'",
        'istartswith': 'STARTING WITH UPPER(%s)',
        'iendswith': "LIKE UPPER(%s) ESCAPE'\\'",
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        
        self._server_version = None
        self.features = DatabaseFeatures()
        self.ops = DatabaseOperations()
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation()
        
        
    def ascii_conv_in(self, text):
        if text is not None:  
            return self.dj_ue.smart_str(text, 'ascii')

    def ascii_conv_out(self, text):
        if text is not None:
            return self.dj_ue.smart_unicode(text)   

    def blob_conv_in(self, text): 
        return self.tc_tu.unicode_conv_in((self.dj_ue.smart_unicode(text), self.FB_CHARSET_CODE))

    def blob_conv_out(self, text):
        return self.tc_tu.unicode_conv_out((text, self.FB_CHARSET_CODE))   

    def fixed_conv_in(self, (val, scale)):
        if val is not None:
            if isinstance(val, basestring):
                val = decimal.Decimal(val)
            return self.tc_fd.fixed_conv_in_precise((val, scale))

    def timestamp_conv_in(self, timestamp):
        if isinstance(timestamp, basestring):
            #Replaces 6 digits microseconds to 4 digits allowed in Firebird
            timestamp = timestamp[:24]
        return self.tc_dt.timestamp_conv_in(timestamp)

    def time_conv_in(self, value):
        import datetime
        if isinstance(value, datetime.datetime):
            value = datetime.time(value.hour, value.minute, value.second, value.microsecond)
        return self.tc_dt.time_conv_in(value)   

    def date_conv_in(self, value):
        if isinstance(value, basestring):
            #Replaces 6 digits microseconds to 4 digits allowed in Firebird
            value = value[:24]
        return self.tc_dt.date_conv_in(value)

    def unicode_conv_in(self, text):
        if text[0] is not None:
            return self.tc_tu.unicode_conv_in((self.dj_ue.smart_unicode(text[0]), self.FB_CHARSET_CODE))

    def _do_connect(self):
        settings_dict = self.settings_dict
        db_options = {}
        conn = {'charset': 'UNICODE_FSS'}            
        if 'DATABASE_OPTIONS' in settings_dict:
            db_options = settings_dict['DATABASE_OPTIONS']
        conn['charset'] = db_options.get('charset', conn['charset'])
        if settings_dict['DATABASE_NAME'] == '':
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured("You need to specify DATABASE_NAME in your Django settings file.")
        conn['dsn'] = settings_dict['DATABASE_NAME']
        if settings_dict['DATABASE_HOST']:
            conn['dsn'] = ('%s:%s') % (settings_dict['DATABASE_HOST'], conn['dsn'])
        if settings_dict['DATABASE_PORT']:
            conn['port'] = settings_dict['DATABASE_PORT']
        if settings_dict['DATABASE_USER']:
            conn['user'] = settings_dict['DATABASE_USER']
        if settings_dict['DATABASE_PASSWORD']:
            conn['password'] = settings_dict['DATABASE_PASSWORD']
        try:
            self.connection = Database.connect(**conn)
        except OperationalError:
            self.connection = Database.create_database(
                "create database '%s' user '%s' password '%s' default character set %s"
                            % (conn['dsn'], conn['user'], conn['password'], conn['charset']))
#        self.connection.set_type_trans_in({
#            'TIMESTAMP' : (lambda s: smart_str(s)[:24]),
#            'BOOLEAN' : (lambda b: 1 if b else 0),
#            'TEXT' : smart_str,
#            'BLOB' : smart_str,
#            })

        self.FB_CHARSET_CODE = 3 #UNICODE_FSS
        if self.connection.charset == 'UTF8':
            self.FB_CHARSET_CODE = 4 # UTF-8 with Firebird 2.0+
        self.connection.set_type_trans_in({
            'DATE':             self.date_conv_in,
            'TIME':             self.time_conv_in,
            'TIMESTAMP':        self.timestamp_conv_in,
            'FIXED':            self.fixed_conv_in,
            'TEXT':             self.ascii_conv_in,
            'TEXT_UNICODE':     self.unicode_conv_in,
            'BLOB':             self.blob_conv_in
        })
        self.connection.set_type_trans_out({
            'DATE':             self.tc_dt.date_conv_out,
            'TIME':             self.tc_dt.time_conv_out,
            'TIMESTAMP':        self.tc_dt.timestamp_conv_out,
            'FIXED':            self.tc_fd.fixed_conv_out_precise,
            'TEXT':             self.ascii_conv_out,
            'TEXT_UNICODE':     self.tc_tu.unicode_conv_out,
            'BLOB':             self.blob_conv_out
        })


    def _cursor(self):
        if self.connection is None:
            self._do_connect()
        cursor = FirebirdCursorWrapper(self.connection)
        return cursor
    
    def get_server_version(self):
        if not self._server_version:
            if not self.connection:
                self.cursor()
            self._server_version = self.connection.server_version
        return self._server_version


class FirebirdCursorWrapper(object):
    """
    Django uses "format" style placeholders, but firebird uses "qmark" style.
    This fixes it -- but note that if you want to use a literal "%s" in a query,
    you'll need to use "%%s".
    
    We need to do some data translation too.
    See: http://kinterbasdb.sourceforge.net/dist_docs/usage.html for Dynamic Type Translation
    """
    
    def __init__(self, connection):
        self.cursor = connection.cursor()

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)
    
    def execute(self, query, params=()):
        cquery = self.convert_query(query, len(params))
        try:
            return self.cursor.execute(cquery, params)
        except Database.ProgrammingError, e:
            err_no = int(str(e).split()[0].strip(',()'))
            output = ["Execute query error. FB error No. %i" % err_no]
            output.extend(str(e).split("'")[1].split('\\n'))
            output.append("Query:")
            output.append(cquery)
            output.append("Parameters:")
            output.append(str(params))
            if err_no in (-803,):
                raise IntegrityError("\n".join(output))
            raise DatabaseError("\n".join(output))

    def executemany(self, query, param_list):
        #print 'cursor.executemany()', query, param_list
        try:
          query = self.convert_query(query, len(param_list[0]))
          return self.cursor.executemany(query, param_list)
        except (IndexError,TypeError):
          # No parameter list provided
          return None

    def convert_query(self, query, num_params):
        return query % tuple("?" * num_params)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchmany(self, size=None):
        return self.cursor.fetchmany(size)

    def fetchall(self):
        return self.cursor.fetchall()


