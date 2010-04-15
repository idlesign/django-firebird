"""
Firebird database backend for Django.

Requires kinterbasdb: http://www.firebirdsql.org/index.php?op=devel&sub=python
"""

import re

try:
    import kinterbasdb as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading kinterbasdb module: %s" % e)

from django.db import utils
from django.db.backends import *
from django.db.backends.signals import connection_created
from django.db.backends.firebird import query
from django.db.backends.firebird.creation import DatabaseCreation
from django.db.backends.firebird.introspection import DatabaseIntrospection
from django.db.backends.firebird.client import DatabaseClient

# Raise exceptions for database warnings if DEBUG is on
from django.conf import settings
#if settings.DEBUG:
#    from warnings import filterwarnings
#    filterwarnings("error", category=Warning)

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
OperationalError = Database.OperationalError

server_version_re = re.compile(r'(\d{1,2})\.(\d{1,2})\.(\d{1,2})')

class CursorWrapper(object):
    """
    A thin wrapper around kinterbasdb cursor class so that we can catch
    particular exception instances and reraise them with the right types.
    
    Django uses "format" style placeholders, but firebird uses "qmark" style.
    This fixes it -- but note that if you want to use a literal "%s" in a query,
    you'll need to use "%%s".
    
    We need to do some data translation too.
    See: http://kinterbasdb.sourceforge.net/dist_docs/usage.html for Dynamic Type Translation
    """
    def __init__(self, cursor):
        self.cursor = cursor
        
    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

    def execute(self, query, args=None):
        try:
            query = self.convert_query(query, len(args))
            return self.cursor.execute(query, args)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]

    def executemany(self, query, args):
        try:
            query = self.convert_query(query, len(args[0]))
            return self.cursor.executemany(query, args)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)), sys.exc_info()[2]
        except Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)), sys.exc_info()[2]

        
    def convert_query(self, query, num_params):
        return query % tuple("?" * num_params)
    
#    def fetchone(self):
#        return self.cursor.fetchone()
#
#    def fetchmany(self, size=None):
#        return self.cursor.fetchmany(size)
#
#    def fetchall(self):
#        return self.cursor.fetchall()

class DatabaseFeatures(BaseDatabaseFeatures):
    """
    This class describes database specific features
    and limitations. 
    """
    uses_custom_query_class = True
    
class DatabaseOperations(BaseDatabaseOperations):
    """
    This class encapsulates all backend-specific differences, such as the way
    a backend performs ordering or calculates the ID of a recently-inserted
    row.
    """

#    def __init__(self):
#        self._engine_version = None
    
    def _get_engine_version(self):
        """ 
        Access method for engine_version property.
        engine_version return a full version in string format 
        (ie: 'WI-V6.3.5.4926 Firebird 1.5' )
        """
        if self._engine_version is None:
            from django.db import connection            
            self._engine_version = connection.get_server_version()
        return self._engine_version
    
    engine_version = property(_get_engine_version)    
    
    def _get_firebird_version(self):
        """ 
        Access method for firebird_version property.
        firebird_version return the version number in a object list format
        Useful for ask for just a part of a version number, for instance, major version is firebird_version[0]  
        """
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
    
    def lookup_cast(self, lookup_type):
        #if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
        if lookup_type in ('iexact', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"
    
    def fulltext_search_sql(self, field_name):
        # We use varchar for TextFields so this is possible
        # Look at http://www.volny.cz/iprenosil/interbase/ip_ib_strings.htm
        return '%%s CONTAINING %s' % self.quote_name(field_name)

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

        self.server_version = None
        self.features = DatabaseFeatures()
        self.ops = DatabaseOperations()
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation(self)

    def _cursor(self):
        new_connection = False
        
        if self.connection is None:
            new_connection = True
            settings_dict = self.settings_dict
            kwargs = {
                'charset': 'UNICODE_FSS',
            }
            if settings_dict['HOST']:
                kwargs['host'] = settings_dict['HOST']
            if settings_dict['NAME']:
                kwargs['database'] = settings_dict['NAME']
            if settings_dict['USER']:
                kwargs['user'] = settings_dict['USER']
            if settings_dict['PASSWORD']:
                kwargs['password'] = settings_dict['PASSWORD']               
            kwargs.update(settings_dict['OPTIONS'])
            self.connection = Database.connect(**kwargs)
            connection_created.send(sender=self.__class__)
            
        cursor = self.connection.cursor()
        
        if new_connection:
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
        
        return CursorWrapper(cursor)
    
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

    def get_server_version(self):
        if not self.server_version:
            if not self._valid_connection():
                self.cursor()
            m = server_version_re.match(self.connection.get_server_info())
            if not m:
                raise Exception('Unable to determine Firebird version from version string %r' % self.connection.get_server_info())
            self.server_version = tuple([int(x) for x in m.groups()])
        return self.server_version
