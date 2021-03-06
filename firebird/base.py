"""
Firebird database backend for Django.

Requires KInterbasDB 3.3+: 
http://www.firebirdsql.org/index.php?op=devel&sub=python
"""

import re
import sys
import base64

try:
    import kinterbasdb as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Unable to load KInterbasDB module: %s" % e)

from django.db import utils
from django.db.backends import *
from django.db.backends.signals import connection_created

from firebird.creation import DatabaseCreation
from firebird.introspection import DatabaseIntrospection
from firebird.client import DatabaseClient

from django.conf import settings

import django.utils.encoding as utils_encoding
import kinterbasdb.typeconv_datetime_stdlib as typeconv_datetime
import kinterbasdb.typeconv_fixed_decimal as typeconv_fixeddecimal
import kinterbasdb.typeconv_text_unicode as typeconv_textunicode

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
OperationalError = Database.OperationalError

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
        
        # This is a workaround for KInterbasDB locks
        if query.find('DROP') != -1:
            # self.cursor.close()
            # someday will recreate cursor here 
            pass
            
        try:
            #print query, args
            if not args:
                args = ()
                return self.cursor.execute(query)
            else:
                query = self.convert_query(query, len(args))
                return self.cursor.execute(query, args)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)+('sql: '+query,)+args), sys.exc_info()[2]
        except Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)+('sql: '+query,)+args), sys.exc_info()[2]

    def executemany(self, query, args):
        try:
            #print query, args
            if not args:
                args = ()
                return self.cursor.executemany(query)
            else:
                query = self.convert_query(query, len(args[0]))
                return self.cursor.executemany(query, args)
        except Database.IntegrityError, e:
            raise utils.IntegrityError, utils.IntegrityError(*tuple(e)+('sql: '+query,)+args), sys.exc_info()[2]
        except Database.DatabaseError, e:
            raise utils.DatabaseError, utils.DatabaseError(*tuple(e)+('sql: '+query,)+args), sys.exc_info()[2]

        
    def convert_query(self, query, num_params):
        return query % tuple("?" * num_params)
    
    def fetchone(self):
        return self.cursor.fetchone()

    def fetchmany(self, size=None):
        return self.cursor.fetchmany(size)

    def fetchall(self):
        return self.cursor.fetchall()
 
class DatabaseFeatures(BaseDatabaseFeatures):
    """
    This class defines bd-specific features.
    
    - can_return_id_from_insert 
        return insert id right in SELECT statements
        as described at http://firebirdfaq.org/faq243/
        for Firebird 2+
    
    """
    can_return_id_from_insert = False
 
class DatabaseOperations(BaseDatabaseOperations):
    """
    This class encapsulates all backend-specific differences, such as the way
    a backend performs ordering or calculates the ID of a recently-inserted
    row.
    """
    compiler_module = 'firebird.compiler'

    def __init__(self, connection, dialect=3):
        super(DatabaseOperations, self).__init__(connection)
        self.dialect = dialect
        self._cache = None
        self._engine_version = None
        self.FB_CHARSET_CODE = 3 #UNICODE_FSS
    
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
        if lookup_type in ('iexact', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"
    
    def fulltext_search_sql(self, field_name):
        # We use varchar for TextFields so this is possible
        # Look at http://www.volny.cz/iprenosil/interbase/ip_ib_strings.htm
        return '%%s CONTAINING %s' % self.quote_name(field_name)

    def return_insert_id(self):
        return 'RETURNING %s', ()

    def last_insert_id(self, cursor, table_name, pk_name):
        # Method used for Firebird prior 2. Method is unreliable, but nothing else could be done
        cursor.execute('SELECT GEN_ID(%s, 0) FROM rdb$database' % (self.get_generator_name(table_name),))
        return cursor.fetchone()[0]

    def max_name_length(self):
        return 31

    def convert_values(self, value, field):
        return super(DatabaseOperations, self).convert_values(value, field)

    def query_class(self, DefaultQueryClass):  
        return query.query_class(DefaultQueryClass)
            
    def quote_name(self, name):
        # Dialect differences as described in http://mc-computing.com/databases/Firebird/SQL_Dialect.html
        if self.dialect==1:
            name = name.upper()
        else:
            if not name.startswith('"') and not name.endswith('"'):
                name = '"%s"' % util.truncate_name(name, self.max_name_length())
            # Handle RDB$DB_KEY calls
            if name.find('RDB$DB_KEY') > -1:
                name = name.strip('"')
        return name

    def get_generator_name(self, table_name):
        return '%s_GN' % util.truncate_name(table_name, self.max_name_length() - 3).upper()

    def get_trigger_name(self, table_name):
        return '%s_TR' % util.truncate_name(table_name, self.max_name_length() - 3).upper()

    def year_lookup_bounds(self, value):
        first = '%s-01-01'
        second = self.conv_in_date('%s-12-31 23:59:59.999999' % value)
        return [first % value, second]
    
    def conv_in_ascii(self, text):
        if text is not None:
            # Handle binary data from RDB$DB_KEY calls
            if text.startswith('base64'):
                return base64.b64decode(text.lstrip('base64'))
            
            return utils_encoding.smart_str(text, 'ascii')   

    def conv_in_blob(self, text): 
        return typeconv_textunicode.unicode_conv_in((utils_encoding.smart_unicode(text), self.FB_CHARSET_CODE))

    def conv_in_fixed(self, (val, scale)):
        if val is not None:
            if isinstance(val, basestring):
                val = decimal.Decimal(val)
            # fixed_conv_in_precise produces weird numbers
            # return typeconv_fixeddecimal.fixed_conv_in_precise((val, scale))
            return int(val.to_integral())

    def conv_in_timestamp(self, timestamp):
        if isinstance(timestamp, basestring):
            # Replaces 6 digits microseconds to 4 digits allowed in Firebird
            timestamp = timestamp[:24]
        return typeconv_datetime.timestamp_conv_in(timestamp)

    def conv_in_time(self, value):
        import datetime
        if isinstance(value, datetime.datetime):
            value = datetime.time(value.hour, value.minute, value.second, value.microsecond)
        return typeconv_datetime.time_conv_in(value)   

    def conv_in_date(self, value):
        if isinstance(value, basestring):
            if self.dialect==1:
                # Replaces 6 digits microseconds to 4 digits allowed in Firebird dialect 1
                value = value[:24]
            else:
                # Time portion is not stored in dialect 3
                value = value[:10]
                
        return typeconv_datetime.date_conv_in(value)

    def conv_in_unicode(self, text):
        if text[0] is not None:
            return typeconv_textunicode.unicode_conv_in((utils_encoding.smart_unicode(text[0]), self.FB_CHARSET_CODE))
        
    def conv_out_ascii(self, text):
        if text is not None:
            # Handle binary data from RDB$DB_KEY calls
            if "\0" in text:
                return 'base64'+base64.b64encode(text)
            
            return utils_encoding.smart_unicode(text, strings_only=True)
        
    def conv_out_blob(self, text):
        return typeconv_textunicode.unicode_conv_out((text, self.FB_CHARSET_CODE))
    
class DatabaseWrapper(BaseDatabaseWrapper):
    """
    Represents a database connection.
    """
    
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

        settings_dict = self.settings_dict
        self.settings = {
            'charset': 'UNICODE_FSS',
            'dialect': 3,
        }
        if settings_dict['HOST']:
            self.settings['host'] = settings_dict['HOST']
        if settings_dict['NAME']:
            self.settings['database'] = settings_dict['NAME']
        if settings_dict['USER']:
            self.settings['user'] = settings_dict['USER']
        if settings_dict['PASSWORD']:
            self.settings['password'] = settings_dict['PASSWORD']               
        self.settings.update(settings_dict['OPTIONS'])
        
        self.dialect = self.settings['dialect']
        
        if 'init_params' in self.settings:
            Database.init(**self.settings['init_params'])

        self.server_version = None
        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self, dialect=self.dialect)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation(self)

    def _cursor(self):
        new_connection = False
        
        if self.connection is None:
            new_connection = True
            self.connection = Database.connect(**self.settings)
            connection_created.send(sender=self.__class__)
             
        cursor = self.connection.cursor()
        
        if new_connection:
            if self.connection.charset == 'UTF8':
                self.ops.FB_CHARSET_CODE = 4 # UTF-8 with Firebird 2.0+

            self.connection.set_type_trans_in({
                'DATE':             self.ops.conv_in_date,
                'TIME':             self.ops.conv_in_time,
                'TIMESTAMP':        self.ops.conv_in_timestamp,
                'FIXED':            self.ops.conv_in_fixed,
                'TEXT':             self.ops.conv_in_ascii,
                'TEXT_UNICODE':     self.ops.conv_in_unicode,
                'BLOB':             self.ops.conv_in_blob
            })
            self.connection.set_type_trans_out({
                'DATE':             typeconv_datetime.date_conv_out,
                'TIME':             typeconv_datetime.time_conv_out,
                'TIMESTAMP':        typeconv_datetime.timestamp_conv_out,
                'FIXED':            typeconv_fixeddecimal.fixed_conv_out_precise,
                'TEXT':             self.ops.conv_out_ascii,
                'TEXT_UNICODE':     typeconv_textunicode.unicode_conv_out,
                'BLOB':             self.ops.conv_out_blob
            })
            
            version = re.search(r'\s(\d{1,2})\.(\d{1,2})', self.connection.server_version)
            self.server_version = tuple([int(x) for x in version.groups()])
            
            # feature for Firebird version 2 and above
            if self.server_version[0] >=2:
                self.features.can_return_id_from_insert = True
        
        return CursorWrapper(cursor)

    def get_server_version(self):           
        return self.server_version