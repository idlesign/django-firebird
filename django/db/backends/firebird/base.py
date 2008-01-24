"""
Firebird database backend for Django.

Requires KInterbasDB 3.2: http://kinterbasdb.sourceforge.net/
The egenix mx (mx.DateTime) is NOT required

Database charset should be UNICODE_FSS or UTF8 (FireBird 2.0+)
To use UTF8 encoding add FIREBIRD_CHARSET = 'UTF8' to your settings.py 
UNICODE_FSS works with all versions and uses less memory
"""

from django.db.backends import BaseDatabaseWrapper, BaseDatabaseFeatures, BaseDatabaseOperations, util
import sys
try:
    import decimal
except ImportError:
    from django.utils import _decimal as decimal    # for Python 2.3

try:
    import kinterbasdb as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured, "Error loading KInterbasDB module: %s" % e

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError

class DatabaseFeatures(BaseDatabaseFeatures):
    inline_fk_references = False 
    needs_datetime_string_cast = False
    needs_upper_for_iops = True
    quote_autofields = True
    uses_custom_field = True
    uses_custom_queryset = True

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
    
    def get_generator_name(self, name):
        return '%s$G' % util.truncate_name(name.strip('"'), self._max_name_length-2).upper()
        
    def get_trigger_name(self, name):
        return '%s$T' % util.truncate_name(name.strip('"'), self._max_name_length-2).upper() 
    
    def _get_firebird_version(self):
        if self._firebird_version is None:
            from django.db import connection
            self._firebird_version = [int(val) for val in connection.server_version.split()[-1].split('.')]
        return self._firebird_version
    firebird_version = property(_get_firebird_version)
  
    def reference_name(self, r_col, col, r_table, table):
        base_name = util.truncate_name('%s$%s' % (r_col, col), self._max_name_length-5)
        return ('%s$%x' % (base_name, abs(hash((r_table, table))))).upper()
    
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
        name = '"%s"' % util.truncate_name(name.strip('"'), self._max_name_length)
        return name
    
    def quote_id_plus_number(self, name):
        try:
            return '"%s" + %s' % tuple(s.strip() for s in name.strip('"').split('+'))
        except:
            return self.quote_name(name)
    
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

    def datetime_cast_sql(self):
        return None

    def drop_sequence_sql(self, table):
        return "DROP GENERATOR %s;" % self.get_generator_name(table)
    
    def drop_foreignkey_sql(self):
        return "DROP CONSTRAINT"
        
    def limit_offset_sql(self, limit, offset=None):
        # limits are handled in custom FirebirdQuerySet 
        assert False, 'Limits are handled in a different way in Firebird'
        return ""

    def random_function_sql(self):
        return "rand()"

    def pk_default_value(self):
        """
        Returns the value to use during an INSERT statement to specify that
        the field should use its default value.
        """
        return 'NULL'
    
    def start_transaction_sql(self):
        return ""

    def fulltext_search_sql(self, field_name):
        # We use varchar for TextFields so this is possible
        # Look at http://www.volny.cz/iprenosil/interbase/ip_ib_strings.htm
        return '%%s CONTAINING %s' % self.quote_name(field_name)

    ############################################################################
    # Advanced SQL ops:
    def autoinc_sql(self, style, table_name, column_name):
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
        
        generator_sql = "%s %s;" % ( style.SQL_KEYWORD('CREATE GENERATOR'), 
                                     generator_name)      
        trigger_sql = "\n".join([
            "%s %s %s %s" % ( \
            style.SQL_KEYWORD('CREATE TRIGGER'), trigger_name, style.SQL_KEYWORD('FOR'),
            style.SQL_TABLE(table_name)),
            "%s 0 %s" % (style.SQL_KEYWORD('ACTIVE BEFORE INSERT POSITION'), style.SQL_KEYWORD('AS')),
            style.SQL_KEYWORD('BEGIN'), 
            "  %s ((%s.%s %s) %s (%s.%s = 0)) %s" % ( \
                style.SQL_KEYWORD('IF'),
                style.SQL_KEYWORD('NEW'), style.SQL_FIELD(column_name), style.SQL_KEYWORD('IS NULL'),
                style.SQL_KEYWORD('OR'), style.SQL_KEYWORD('NEW'), style.SQL_FIELD(column_name),
                style.SQL_KEYWORD('THEN')
            ),
            "  %s" % style.SQL_KEYWORD('BEGIN'), 
            "    %s.%s = %s(%s, 1);" % ( \
                style.SQL_KEYWORD('NEW'), style.SQL_FIELD(column_name),
                style.SQL_KEYWORD('GEN_ID'), generator_name
            ),
            "  %s" % style.SQL_KEYWORD('END'),
            "%s;" % style.SQL_KEYWORD('END')])
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
            
    ############################################################################
    # Custom classes
    def field_class(this, DefaultField):
        from django.db import connection
        from django.db.models.fields import prep_for_like_query
        class FirebirdField(DefaultField):
            def get_db_prep_lookup(self, lookup_type, value):       
                "Returns field's value prepared for database lookup."
                if lookup_type in ('exact', 'regex', 'iregex', 'gt', 'gte', 'lt', 
                    'lte', 'month', 'day', 'search', 'icontains', 
                    'startswith', 'istartswith'):
                    return [value]
                elif lookup_type in ('range', 'in'):
                    return value
                elif lookup_type in ('contains',):
                    return ["%%%s%%" % prep_for_like_query(value)]
                elif lookup_type == 'iexact':
                    return [prep_for_like_query(value)]
                elif lookup_type in ('endswith', 'iendswith'):
                    return ["%%%s" % prep_for_like_query(value)]
                elif lookup_type == 'isnull':
                    return []
                elif lookup_type == 'year':
                    try:
                        value = int(value)
                    except ValueError:
                        raise ValueError("The __year lookup type requires an integer argument")
                    return ['%s-01-01 00:00:00' % value, '%s-12-31 23:59:59.999999' % value]
                raise TypeError("Field has invalid lookup: %s" % lookup_type)
        return FirebirdField

    def query_set_class(this, DefaultQuerySet):
        from django.db import connection
        from django.db.models.query import EmptyResultSet, GET_ITERATOR_CHUNK_SIZE
        
        class FirebirdQuerySet(DefaultQuerySet):
            __firefilter__ = None
            ___fireargs__ = ()
            def _get_sql_clause(self):
                from django.db.models.query import SortedDict, handle_legacy_orderlist, orderfield2column, fill_table_cache
                qn = this.quote_name
                opts = self.model._meta

                # Construct the fundamental parts of the query: SELECT X FROM Y WHERE Z.
                select = ["%s.%s" % (qn(opts.db_table), qn(f.column)) for f in opts.fields]
                tables = [qn(t) for t in self._tables]
                joins = SortedDict()
                where = self._where[:]
                params = self._params[:]

                # Convert self._filters into SQL.
                joins2, where2, params2 = self._filters.get_sql(opts)
                joins.update(joins2)
                where.extend(where2)
                params.extend(params2)

                # Add additional tables and WHERE clauses based on select_related.
                if self._select_related:
                    fill_table_cache(opts, select, tables, where,
                                     old_prefix=opts.db_table,
                                     cache_tables_seen=[opts.db_table],
                                     max_depth=self._max_related_depth)
                
                # Add any additional SELECTs.
                if self._select:
                    select.extend([('(%s AS %s') % (qn(s[1]), qn(s[0])) for s in self._select.items()])

                # Start composing the body of the SQL statement.
                sql = [" FROM", qn(opts.db_table)]

                # Compose the join dictionary into SQL describing the joins.
                if joins:
                    sql.append(" ".join(["%s %s %s ON %s" % (join_type, table, alias, condition)
                                    for (alias, (table, join_type, condition)) in joins.items()]))

                # Compose the tables clause into SQL.
                if tables:
                    sql.append(", " + ", ".join(tables))

                # Compose the where clause into SQL.
                if where: 
                    sql.append(where and "WHERE " + " AND ".join(where))

                # ORDER BY clause
                order_by = []
                if self._order_by is not None:
                    ordering_to_use = self._order_by
                else:
                    ordering_to_use = opts.ordering
                for f in handle_legacy_orderlist(ordering_to_use):
                    if f == '?': # Special case.
                        order_by.append(connection.ops.random_function_sql())
                    else:
                        if f.startswith('-'):
                            col_name = f[1:]
                            order = "DESC"
                        else:
                            col_name = f
                            order = "ASC"
                        if "." in col_name:
                            table_prefix, col_name = col_name.split('.', 1)
                            table_prefix = qn(table_prefix) + '.'
                        else:
                            # Use the database table as a column prefix if it wasn't given,
                            # and if the requested column isn't a custom SELECT.
                            if "." not in col_name and col_name not in (self._select or ()):
                                table_prefix = qn(opts.db_table) + '.'
                            else:
                                table_prefix = ''
                        order_by.append('%s%s %s' % \
                            (table_prefix, qn(orderfield2column(col_name, opts)), order))
                if order_by:
                    sql.append("ORDER BY " + ", ".join(order_by))

                return select, " ".join(sql), params
            
            def iterator(self):
                "Performs the SELECT database lookup of this QuerySet."
                from django.db.models.query import get_cached_row
                if self.__firefilter__:
                    extra_select = self._select.items()
                    cursor = connection.cursor()
                    self.__firefilter__.execute(cursor, *self.__fireargs__)
                else:    
                    try:
                        select, sql, params = self._get_sql_clause()
                    except EmptyResultSet:
                        raise StopIteration 
                        
                    # self._select is a dictionary, and dictionaries' key order is
                    # undefined, so we convert it to a list of tuples.
                    extra_select = self._select.items()
                    cursor = connection.cursor() 
                    limit_offset_before = "" 
                    if self._limit is not None: 
                        limit_offset_before += "FIRST %s " % self._limit 
                        if self._offset: 
                            limit_offset_before += "SKIP %s " % self._offset
                    else:
                        assert self._offset is None, "'offset' is not allowed without 'limit'"
                    cursor.execute("SELECT " + limit_offset_before + (self._distinct and "DISTINCT " or "") + ",".join(select) + sql, params)
                    
                fill_cache = self._select_related
                fields = self.model._meta.fields
                index_end = len(fields)
                while 1:
                    rows = cursor.fetchmany(GET_ITERATOR_CHUNK_SIZE)
                    if not rows:
                        raise StopIteration
                    for row in rows:
                        row = self.resolve_columns(row, fields)
                        if fill_cache:
                            obj, index_end = get_cached_row(klass=self.model, row=row,
                                                            index_start=0, max_depth=self._max_related_depth)
                        else:
                            obj = self.model(*row[:index_end])
                        for i, k in enumerate(extra_select):
                            setattr(obj, k[0], row[index_end+i])
                        yield obj
            
            def resolve_columns(self, row, fields=()):
                from django.db.models.fields import DateField, DateTimeField, \
                    TimeField, BooleanField, NullBooleanField, DecimalField, Field
                values = []
                for value, field in map(None, row, fields):
                    # Convert 1 or 0 to True or False
                    if value in (1, 0) and isinstance(field, (BooleanField, NullBooleanField)):
                        value = bool(value)

                    values.append(value)
                return values
            
            def firefilter(self, proc, *args, **kwargs):
                assert not kwargs, "Keyword arguments not supported with stored procedures"
                if len(args) > 0:
                    assert self._limit is None and self._offset is None, \
                        "Cannot filter a query once a slice has been taken."
                clone = self._clone()
                clone.__firefilter__ = proc
                clone.__fireargs__ = args
                return clone
                
            def extra(self, select=None, where=None, params=None, tables=None):
                assert self._limit is None and self._offset is None, \
                        "Cannot change a query once a slice has been taken"
                clone = self._clone()
                qn = this.quote_name
                if select: clone._select.update(select)
                if where:
                    qn_where = []
                    for where_item in where:
                        try:
                            table, col_exact = where_item.split(".")
                            col, value = col_exact.split("=")
                            where_item = "%s.%s = %s" % (qn(table.strip()), 
                                qn(col.strip()), value.strip())
                        except:
                            try:
                                table, value = where_item.split("=")
                                where_item = "%s = %s" % (qn(table.strip()), qn(value.strip()))
                            except:
                                raise TypeError, "Can't understand extra WHERE clause: %s" % where 
                        qn_where.append(where_item)
                    clone._where.extend(qn_where)
                if params: clone._params.extend(params)
                if tables: clone._tables.extend(tables)
                return clone
                
        return FirebirdQuerySet

################################################################################
# Cursor wrapper        
class FirebirdCursorWrapper(object):
    """
    Django uses "format" ('%s') style placeholders, but firebird uses "qmark" ('?') style.
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

    def unicode_conv_in(self, text):
        if text[0] is not None:
            return self.tc_tu.unicode_conv_in((self.dj_ue.smart_unicode(text[0]), self.FB_CHARSET_CODE))

    def __init__(self, cursor, connection):   
        self.cursor = cursor
        self._connection = connection
        self._statement = None #prepared statement
        self.FB_CHARSET_CODE = 3 #UNICODE_FSS
        if connection.charset == 'UTF8':
            self.FB_CHARSET_CODE = 4 # UTF-8 with Firebird 2.0+
        self.cursor.set_type_trans_in({
            'DATE':             self.tc_dt.date_conv_in,
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
    
    def execute_immediate(self, query, params=()):
        query = query % tuple(params)
        self._connection.execute_immediate(query)
    
    def prepare(self, query):
        """
        Returns prepared statement for use with execute_prepared 
        http://kinterbasdb.sourceforge.net/dist_docs/usage.html#adv_prepared_statements
        """
        query.replace("%s", "?")
        return self.cursor.prep(query)
    
    def execute_prepared(self, statement, params):
        return self.cursor.execute(statement, params)
    
    def execute_straight(self, query, params=()):
        """
        Kinterbasdb-style execute with '?' instead of '%s'
        """
        try:
            return self.cursor.execute(query, params)
        except Database.ProgrammingError, e:
            err_no = int(str(e).split()[0].strip(',()'))
            output = ["Execute query error. FB error No. %i" % err_no]
            output.extend(str(e).split("'")[1].split('\\n'))
            output.append("Query:")
            output.append(query)
            output.append("Parameters:")
            output.append(str(params))
            raise Database.ProgrammingError, "\n".join(output)
    
    def execute(self, query, params=()):
        cquery = self.convert_query(query, len(params))
        if self._get_query() != cquery:
            try:
                self._statement = self.cursor.prep(cquery)
            except Database.ProgrammingError, e:
                output = ["Prepare query error."]
                output.extend(str(e).split("'")[1].split('\\n'))
                output.append("Query:")
                output.append(cquery)
                raise Database.ProgrammingError, "\n".join(output)
        try:
            return self.cursor.execute(self._statement, params)
        except Database.ProgrammingError, e:
            err_no = int(str(e).split()[0].strip(',()'))
            output = ["Execute query error. FB error No. %i" % err_no]
            output.extend(str(e).split("'")[1].split('\\n'))
            output.append("Query:")
            output.append(cquery)
            output.append("Parameters:")
            output.append(str(params))
            raise Database.ProgrammingError, "\n".join(output)
    
    def executemany(self, query, param_list):
        try:
            cquery = self.convert_query(query, len(param_list[0]))
        except IndexError:
            return None
        if self._get_query() != cquery:
            self._statement = self.cursor.prep(cquery)
        return self.cursor.executemany(self._statement, param_list)

    def convert_query(self, query, num_params):
        try:
            return query % tuple("?" * num_params)
        except TypeError, e:
            print query, num_params
            raise TypeError, e
    
    def _get_query(self):
        if self._statement:
            return self._statement.sql
    
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
        self. _current_cursor = None
        self._raw_cursor = None
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
        self._raw_cursor = cursor
        cursor = FirebirdCursorWrapper(cursor, self)
        self._current_cursor = cursor
        return cursor

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.connection, attr)

