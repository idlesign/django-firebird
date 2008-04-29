"""
Firebird database backend for Django.

Requires KInterbasDB 3.2: http://kinterbasdb.sourceforge.net/
The egenix mx (mx.DateTime) is NOT required

Database charset should be UNICODE_FSS or UTF8 (FireBird 2.0+)
To use UTF8 encoding add FIREBIRD_CHARSET = 'UTF8' to your settings.py 
UNICODE_FSS works with all versions and uses less memory
"""

import sys
import datetime
try:
    import decimal
except ImportError:
    from django.utils import _decimal as decimal    # for Python 2.3

from django.db.backends import BaseDatabaseWrapper, BaseDatabaseFeatures, BaseDatabaseOperations, util
from django.db.backends.firebird.creation import TEST_MODE
from django.db.backends.firebird import query

try:
    import kinterbasdb as Database
    Database.init(type_conv=200)
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured, "Error loading KInterbasDB module: %s" % e

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError


class DatabaseFeatures(BaseDatabaseFeatures):
    allows_unique_and_pk = False
    always_quote = True
    inline_fk_references = False 
    needs_datetime_string_cast = False
    needs_upper_for_iops = True
    order_by_ordinal = True
    uses_custom_lookups = True
    uses_custom_query_class = True
    supports_constraints = TEST_MODE < 2

################################################################################
# Database operations (db.connection.ops)  
class DatabaseOperations(BaseDatabaseOperations):
    """
    This class encapsulates all backend-specific differences, such as the way
    a backend performs ordering or calculates the ID of a recently-inserted
    row.
    """
    # Utility ops: names, version, page size etc.: 
    _max_name_length = 31
    def __init__(self):
        self._firebird_version = None
        self._page_size = None
        self._quote_cache = {}
    
    def get_generator_name(self, name):
        return '%s$G' % util.truncate_name(name.strip('"'), self._max_name_length-2).upper()
        
    def get_trigger_name(self, name):
        return '%s$T' % util.truncate_name(name.strip('"'), self._max_name_length-2).upper() 
    
    def _get_firebird_version(self):
        if self._firebird_version is None:
            from django.db import connection
            if not connection.connection:
                connection.cursor()
            self._firebird_version = [int(val) for val in connection.server_version.split()[-1].split('.')]
        return self._firebird_version
    firebird_version = property(_get_firebird_version)
  
    def reference_name(self, r_col, col, r_table, table):
        base_name = util.truncate_name('%s$%s' % (r_col, col), self._max_name_length-5)
        return util.truncate_name(('%s$%x' % (base_name, abs(hash((r_table, table))))), self._max_name_length).upper()
    
    def _get_page_size(self):
        if self._page_size is None:
            from django.db import connection
            self._page_size = connection.database_info(Database.isc_info_page_size, 'i')
        return self._page_size
    page_size = property(_get_page_size)
    
    def _get_index_limit(self):
        if self.firebird_version[0] < 2:
            self._index_limit = 252 
        else:
            page_size = self._get_page_size()
            self._index_limit = page_size/4
        return self._index_limit
    index_limit = property(_get_index_limit)
    
    def max_name_length(self):
        return self._max_name_length
    
    def quote_name(self, name):
        try:
            return self._quote_cache[name]
        except KeyError:
            pass
        try:
            name2 = int(name)
            self._quote_cache[name] = name2
            return name2
        except ValueError:
            pass
        if name == '%s':
            self._quote_cache[name] = '%s__FB_MERGE__'
            return '%s__FB_MERGE__'
        name2 = '"%s"' % util.truncate_name(name.strip('"'), self._max_name_length)
        self._quote_cache[name] = name2
        return name2

    def field_cast_sql(self, db_type):
        return '%s'

    ############################################################################
    # Basic SQL ops:    
    def last_insert_id(self, cursor, table_name, pk_name=None):
        generator_name = self.get_generator_name(table_name)
        cursor.execute('SELECT GEN_ID(%s, 0) from RDB$DATABASE' % generator_name)
        return cursor.fetchone()[0]

    def date_extract_sql(self, lookup_type, column_name):
        # lookup_type is 'year', 'month', 'day'
        return "EXTRACT(%s FROM %s)" % (lookup_type, column_name)

    def date_trunc_sql(self, lookup_type, column_name):
        if lookup_type == 'year':
             sql = "EXTRACT(year FROM %s)||'-01-01 00:00:00'" % column_name
        elif lookup_type == 'month':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-01 00:00:00'" % (column_name, column_name)
        elif lookup_type == 'day':
            sql = "EXTRACT(year FROM %s)||'-'||EXTRACT(month FROM %s)||'-'||EXTRACT(day FROM %s)||' 00:00:00'" % (column_name, column_name, column_name)
        return "CAST(%s AS TIMESTAMP)" % sql

    #def datetime_cast_sql(self):
    #    return None

    def drop_sequence_sql(self, table):
        return "DROP GENERATOR %s;" % self.get_generator_name(table)

    def drop_foreignkey_sql(self):
        return "DROP CONSTRAINT"
        
    def limit_offset_sql(self, limit, offset=None):
        return ''

    def random_function_sql(self):
        return "rand()"

    def pk_default_value(self):
        """
        Returns the value to use during an INSERT statement to specify that
        the field should use its default value.
        """
        return 'NULL'

    def lookup_cast(self, lookup_type):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"

    def start_transaction_sql(self):
        return ""

    def fulltext_search_sql(self, field_name):
        # We use varchar for TextFields so this is possible
        # Look at http://www.volny.cz/iprenosil/interbase/ip_ib_strings.htm
        return '%%s CONTAINING %s' % self.quote_name(field_name)

    ############################################################################
    # Advanced SQL ops:
    def autoinc_sql(self, table_name, column_name):
        """
        To simulate auto-incrementing primary keys in Firebird, we have to
        create a generator and a trigger.

        Create the generators and triggers names based only on table name
        since django only support one auto field per model
        """
        generator_name = self.get_generator_name(table_name)
        trigger_name = self.get_trigger_name(table_name)
        column_name = self.quote_name(column_name)
        table_name = self.quote_name(table_name)

        generator_sql = "CREATE GENERATOR %s;" % generator_name   
        trigger_sql = "\n".join([
            "CREATE TRIGGER %s FOR %s" %  (trigger_name, table_name),
            "ACTIVE BEFORE INSERT POSITION 0 AS",
            "BEGIN", 
            "  IF ((NEW.%s IS NULL) OR (NEW.%s = 0)) THEN" % (column_name, column_name),
            "  BEGIN", 
            "    NEW.%s = GEN_ID(%s, 1);" % (column_name, generator_name),
            "  END",
            "END;"])
        return (generator_sql, trigger_sql)

    def sequence_reset_sql(self, style, model_list):
        from django.db import models
        output = []
        sql = ['%s %s %s' % (style.SQL_KEYWORD('CREATE OR ALTER PROCEDURE'),
                             style.SQL_TABLE('"GENERATOR_RESET"'),
                             style.SQL_KEYWORD('AS'))]
        sql.append('%s %s' % (style.SQL_KEYWORD('DECLARE VARIABLE'), style.SQL_COLTYPE('start_val integer;')))
        sql.append('%s %s' % (style.SQL_KEYWORD('DECLARE VARIABLE'), style.SQL_COLTYPE('gen_val integer;')))
        sql.append('\t%s' % style.SQL_KEYWORD('BEGIN'))
        sql.append('\t\t%s %s %s %s %s %s;' % (style.SQL_KEYWORD('SELECT MAX'), style.SQL_FIELD('(%(col)s)'),
                                           style.SQL_KEYWORD('FROM'), style.SQL_TABLE('%(table)s'),
                                           style.SQL_KEYWORD('INTO'), style.SQL_COLTYPE(':start_val')))
        sql.append('\t\t%s (%s %s) %s' % (style.SQL_KEYWORD('IF'), style.SQL_COLTYPE('start_val'),
                                    style.SQL_KEYWORD('IS NULL'), style.SQL_KEYWORD('THEN')))
        sql.append('\t\t\t%s = %s(%s, 1 - %s(%s, 0));' %\
            (style.SQL_COLTYPE('gen_val'), style.SQL_KEYWORD('GEN_ID'), style.SQL_TABLE('%(gen)s'),
             style.SQL_KEYWORD('GEN_ID'), style.SQL_TABLE('%(gen)s')))
        sql.append('\t\t%s' % style.SQL_KEYWORD('ELSE'))
        sql.append('\t\t\t%s = %s(%s, %s - %s(%s, 0));' %\
            (style.SQL_COLTYPE('gen_val'), style.SQL_KEYWORD('GEN_ID'),
             style.SQL_TABLE('%(gen)s'), style.SQL_COLTYPE('start_val'), style.SQL_KEYWORD('GEN_ID'),
             style.SQL_TABLE('%(gen)s')))
        sql.append('\t\t%s;' % style.SQL_KEYWORD('EXIT'))
        sql.append('%s;' % style.SQL_KEYWORD('END'))
        sql ="\n".join(sql)
        for model in model_list:
            for f in model._meta.fields:
                if isinstance(f, models.AutoField):
                    generator_name = self.get_generator_name(model._meta.db_table)
                    column_name = self.quote_name(f.db_column or f.name)
                    table_name = self.quote_name(model._meta.db_table)
                    output.append(sql % {'col' : column_name, 'table' : table_name, 'gen' : generator_name})
                    output.append('%s %s;' % (style.SQL_KEYWORD('EXECUTE PROCEDURE'), 
                                              style.SQL_TABLE('"GENERATOR_RESET"')))
                    break # Only one AutoField is allowed per model, so don't bother continuing.
            for f in model._meta.many_to_many:
                generator_name = self.get_generator_name(f.m2m_db_table())
                table_name = self.quote_name(f.m2m_db_table())
                column_name = '"id"'
                output.append(sql % {'col' : column_name, 'table' : table_name, 'gen' : generator_name})
                output.append('%s %s;' % (style.SQL_KEYWORD('EXECUTE PROCEDURE'), 
                                          style.SQL_TABLE('"GENERATOR_RESET"')))
        return output
    
    def sql_flush(self, style, tables, sequences):
        if tables:
            sql = ['%s %s %s;' % \
                    (style.SQL_KEYWORD('DELETE'),
                     style.SQL_KEYWORD('FROM'),
                     style.SQL_TABLE(self.quote_name(table))
                     ) for table in tables]
            for generator_info in sequences:
                table_name = generator_info['table']
                query = "%s %s %s 0;" % (style.SQL_KEYWORD('SET GENERATOR'), 
                    self.get_generator_name(table_name), style.SQL_KEYWORD('TO'))
                sql.append(query)
            return sql
        else:
            return []

    def get_db_prep_lookup(self, lookup_type, value):
        "Returns field's value prepared for database lookup."
        from django.db.models.fields import prep_for_like_query, QueryWrapper
        if hasattr(value, 'as_sql'):
            sql, params = value.as_sql()
            return QueryWrapper(('(%s)' % sql), params)
        if lookup_type in ('exact', 'iexact', 'regex', 'iregex', 'gt', 'gte', 'lt', 
            'lte', 'month', 'day', 'search', 'icontains', 
            'startswith', 'istartswith'):
            return [value]
        elif lookup_type in ('range', 'in'):
            return value
        elif lookup_type in ('contains',):
            return ["%%%s%%" % prep_for_like_query(value)]
        elif lookup_type in ('endswith', 'iendswith'):
            return ["%%%s" % prep_for_like_query(value)]
        elif lookup_type == 'isnull':
            return []
        elif lookup_type == 'year':
            try:
                value = int(value)
            except ValueError:
                raise ValueError("The __year lookup type requires an integer argument")
            return [datetime.datetime(value, 1, 1, 0, 0, 0), datetime.datetime(value, 12, 31, 23, 59, 59, 999999)]
        raise TypeError("Field has invalid lookup: %s" % lookup_type)

    def query_class(self, DefaultQueryClass):
        return query.query_class(DefaultQueryClass, Database)

################################################################################
# Cursor wrapper        
class FirebirdCursorWrapper(object):
    """
    Django uses "format" ('%s') style placeholders, but Firebird uses "qmark" ('?') style.
    This fixes it -- but note that if you want to use a literal "%s" in a query,
    you'll need to use "%%s".
    
    We also do all automatic type conversions here.
    """
    import kinterbasdb.typeconv_datetime_stdlib as tc_dt
    import kinterbasdb.typeconv_fixed_decimal as tc_fd
    import kinterbasdb.typeconv_text_unicode as tc_tu
    import django.utils.encoding as dj_ue

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

    def __init__(self, cursor, connection):   
        self.cursor = cursor
        self._connection = connection
        self.FB_CHARSET_CODE = 3 #UNICODE_FSS
        if connection.charset == 'UTF8':
            self.FB_CHARSET_CODE = 4 # UTF-8 with Firebird 2.0+
        self.cursor.set_type_trans_in({
            'DATE':             self.date_conv_in,
            'TIME':             self.time_conv_in,
            'TIMESTAMP':        self.timestamp_conv_in,
            'FIXED':            self.fixed_conv_in,
            'TEXT':             self.ascii_conv_in,
            'TEXT_UNICODE':     self.unicode_conv_in,
            'BLOB':             self.blob_conv_in
        })
        self.cursor.set_type_trans_out({
            'DATE':             self.tc_dt.date_conv_out,
            'TIME':             self.tc_dt.time_conv_out,
            'TIMESTAMP':        self.tc_dt.timestamp_conv_out,
            'FIXED':            self.tc_fd.fixed_conv_out_precise,
            'TEXT':             self.ascii_conv_out,
            'TEXT_UNICODE':     self.tc_tu.unicode_conv_out,
            'BLOB':             self.blob_conv_out
        })

    def execute(self, query, params=()):
        if query.find('__FB_MERGE__') > 0:
            cquery = query.replace('__FB_MERGE__', '') % params
            params = ()
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
        try:
            cquery = self.convert_query(query, len(param_list[0]))
        except IndexError:
            return None
        return self.cursor.executemany(cquery, param_list)

    def convert_query(self, query, num_params):
        return query % tuple("?" * num_params)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchmany(self, size=None):
        if size is None:
            return self.cursor.fetchmany()
        return self.cursor.fetchmany(size)

    def fetchall(self):
        return self.cursor.fetchall()

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

################################################################################
# DatabaseWrapper(db.connection)  
class DatabaseWrapper(BaseDatabaseWrapper):
    features = DatabaseFeatures()
    ops = DatabaseOperations()
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
        'iendswith': "LIKE UPPER(%s) ESCAPE'\\'"
    }

    def __init__(self, **kwargs):
        from django.conf import settings
        super(DatabaseWrapper, self).__init__(**kwargs)
        self.charset = 'UNICODE_FSS'
        self.FB_MAX_VARCHAR = 10921 #32765 MAX /3
        self.BYTES_PER_DEFAULT_CHAR = 3
        if hasattr(settings, 'FIREBIRD_CHARSET'):
            if settings.FIREBIRD_CHARSET == 'UTF8':
                self.charset = 'UTF8' 
                self.FB_MAX_VARCHAR = 8191 #32765 MAX /4
                self.BYTES_PER_DEFAULT_CHAR = 4
        
    def _connect(self, settings):
        if settings.DATABASE_NAME == '':
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured, "You need to specify DATABASE_NAME in your Django settings file."
        kwargs = {'charset' : self.charset }
        if settings.DATABASE_HOST:
            kwargs['dsn'] = "%s:%s" % (settings.DATABASE_HOST, settings.DATABASE_NAME)
        else:
            kwargs['dsn'] = "localhost:%s" % settings.DATABASE_NAME
        if settings.DATABASE_USER:
            kwargs['user'] = settings.DATABASE_USER
        if settings.DATABASE_PASSWORD:
            kwargs['password'] = settings.DATABASE_PASSWORD
        self.connection = Database.connect(**kwargs)
        assert self.connection.charset == self.charset

    def cursor(self):
        from django.conf import settings
        cursor = self._cursor(settings)
        if settings.DEBUG:
            self._debug_cursor = self.make_debug_cursor(cursor)
            return self._debug_cursor
        return cursor

    def _cursor(self, settings):
        if self.connection is None:
            self._connect(settings)
        cursor = self.connection.cursor()
        cursor = FirebirdCursorWrapper(cursor, self)
        return cursor

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.connection, attr)

