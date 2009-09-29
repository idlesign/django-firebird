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
from django.db.backends.firebird.client import DatabaseClient
from django.db.backends.firebird.creation import DatabaseCreation
from django.db.backends.firebird.introspection import DatabaseIntrospection
from django.utils.encoding import smart_str, smart_unicode, force_unicode


DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError
OperationalError = Database.OperationalError

class DatabaseFeatures(BaseDatabaseFeatures):
    uses_custom_query_class = True

class DatabaseOperations(BaseDatabaseOperations):

    def autoinc_sql(self, table, column):
        # To simulate auto-incrementing primary keys in Oracle, we have to
        # create a sequence and a trigger.
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
        if lookup_type == 'week_day':
            # TO_CHAR(field, 'D') returns an integer from 1-7, where 1=Sunday.
            return "TO_CHAR(%s, 'D')" % field_name
        else:
            return "EXTRACT(%s FROM %s)" % (lookup_type.upper(), field_name)

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
    
#    def lookup_cast(self, lookup_type):
#        #if lookup_type in ('icontains'):
#        print self.connections.ops
#        return '%s'

    def query_class(self, DefaultQueryClass):  
              
        class FirebirdQuery(DefaultQueryClass):                           
            def as_sql(self, with_limits=False, with_col_aliases=False):
                """
                Return custom SQL. Use FIRST and SKIP statement instead of
                LIMIT and OFFSET.
                """
                self.pre_sql_setup()
                out_cols = self.get_columns(with_col_aliases)
                ordering, ordering_group_by = self.get_ordering()

                from_, f_params = self.get_from_clause()

                qn = self.quote_name_unless_alias
                where, w_params = self.where.as_sql(qn=qn)
                
                # Fix for icontins filter option.
                # See http://code.google.com/p/django-firebird/issues/detail?id=4
                # I don't like this solution so much. But... it's work. Need more test.                
                
                #if 'CONTAINING' in where:
                #    w_params = [w_params[0].replace('%', '')]
                
                having, h_params = self.having.as_sql(qn=qn)
                params = []
                for val in self.extra_select.itervalues():
                    params.extend(val[1])

                result = ['SELECT']
                if with_limits:
                    if self.high_mark is not None:
                        result.append('FIRST %d' % (self.high_mark - self.low_mark))
                    if self.low_mark:
                        if self.high_mark is None:
                            val = self.connection.ops.no_limit_value()
                            if val:
                                result.append('FIRST %d' % val)
                        result.append('SKIP %d' % self.low_mark)
                if self.distinct:
                    result.append('DISTINCT')
                result.append(', '.join(out_cols + self.ordering_aliases))

                result.append('FROM')
                result.extend(from_)
                params.extend(f_params)

                if where:
                    result.append('WHERE %s' % where)
                    params.extend(w_params)
                if self.extra_where:
                    if not where:
                        result.append('WHERE')
                    else:
                        result.append('AND')
                    result.append(' AND '.join(self.extra_where))

                grouping, gb_params = self.get_grouping()
                if grouping:
                    if ordering:
                        # If the backend can't group by PK (i.e., any database
                        # other than MySQL), then any fields mentioned in the
                        # ordering clause needs to be in the group by clause.
                        if not self.connection.features.allows_group_by_pk:
                            for col, col_params in ordering_group_by:
                                if col not in grouping:
                                    grouping.append(str(col))
                                    gb_params.extend(col_params)
                    else:
                        ordering = self.connection.ops.force_no_ordering()
                    result.append('GROUP BY %s' % ', '.join(grouping))
                    params.extend(gb_params)

                if having:
                    result.append('HAVING %s' % having)
                    params.extend(h_params)

                if ordering:
                    result.append('ORDER BY %s' % ', '.join(ordering))

                params.extend(self.extra_params)
                return ' '.join(result), tuple(params)
        return FirebirdQuery

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

        self.features = DatabaseFeatures()
        self.ops = DatabaseOperations()
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation()

    def _cursor(self):
        if self.connection is None:
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
            self.connection.set_type_trans_in({
                'TIMESTAMP' : (lambda s: smart_str(s)[:24]),
                'BOOLEAN' : (lambda b: 1 if b else 0),
                'TEXT' : smart_str,
                'BLOB' : smart_str,
                })
        cursor = FirebirdCursorWrapper(self.connection)
        return cursor

class FirebirdCursorWrapper(object):
    """
    Django uses "format" style placeholders, but firebird uses "qmark" style.
    This fixes it -- but note that if you want to use a literal "%s" in a query,
    you'll need to use "%%s".
    """
    def __init__(self, connection):
        self.cursor = connection.cursor()

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)

    def execute(self, query, params=()):
        #print 'cursor.execute()', query, params
        query = self.convert_query(query, len(params))
        return self.cursor.execute(query, params)

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


