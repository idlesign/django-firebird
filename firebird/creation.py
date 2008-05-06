# This dictionary maps Field objects to their associated Firebird column
# types, as strings. Column-type strings can contain format strings; they'll
# be interpolated against the values of Field.__dict__ before being output.
# If a column type is set to None, it won't be included in the output.

from kinterbasdb import connect, create_database
from django.core.management import call_command
from django.conf import settings

import sys
import os
import re
import codecs
import warnings

try:
    set
except NameError:
    # Python 2.3 fallback
    from sets import Set as set
    from sets import ImmutableSet as frozenset


# Setting TEST_MODE to 2 disables strict FK constraints (for forward/post references)
# Setting TEST_MODE to 0 is the most secure option (it even fails some official Django tests  because of it)
TEST_MODE = 0
if 'FB_DJANGO_TEST_MODE' in os.environ:
    TEST_MODE = int(os.environ['FB_DJANGO_TEST_MODE'])

DATA_TYPES = {
    'AutoField':                     'integer',
    'BooleanField':                  '"BooleanField"',
    'CharField':                     'varchar(%(max_length)s)',
    'CommaSeparatedIntegerField':    'varchar(%(max_length)s) CHARACTER SET ASCII',
    'DateField':                     'date',
    'DateTimeField':                 'timestamp',
    'DecimalField':                  'numeric(%(max_digits)s, %(decimal_places)s)',
    'FileField':                     'varchar(%(max_length)s)',
    'FilePathField':                 'varchar(%(max_length)s)',
    'FloatField':                    'double precision',
    'ImageField':                    'varchar(%(max_length)s)',
    'IntegerField':                  'integer',
    'IPAddressField':                'varchar(15) CHARACTER SET ASCII',
    'NullBooleanField':              '"NullBooleanField"', 
    'OneToOneField':                 'integer',
    'PhoneNumberField':              'varchar(20) CHARACTER SET ASCII', 
    'PositiveIntegerField':          '"PositiveIntegerField"',
    'PositiveSmallIntegerField':     '"PositiveSmallIntegerField"',
    'SlugField':                     'varchar(%(max_length)s)',
    'SmallIntegerField':             'smallint',
    'LargeTextField':                'blob sub_type text',
    'TextField':                     '"TextField"',
    'TimeField':                     'time',
    'URLField':                      'varchar(%(max_length)s) CHARACTER SET ASCII',
    'USStateField':                  'varchar(2) CHARACTER SET ASCII'
}
      
PYTHON_TO_FB_ENCODING_MAP = {
    'ascii':        'ASCII',
    'utf_8':        hasattr(settings, 'FIREBIRD_CHARSET') \
                    and settings.FIREBIRD_CHARSET in ('UNICODE_FSS', 'UTF8') \
                    and settings.FIREBIRD_CHARSET or 'UNICODE_FSS',
    'shift_jis':    'SJIS_0208',
    'euc_jp':       'EUCJ_0208',
    'cp737':        'DOS737',
    'cp437':        'DOS437',
    'cp850':        'DOS850',
    'cp865':        'DOS865',
    'cp860':        'DOS860',
    'cp863':        'DOS863',
    'cp775':        'DOS775',
    'cp862':        'DOS862',
    'cp864':        'DOS864',
    'iso8859_1':    'ISO8859_1',
    'iso8859_2':    'ISO8859_2',
    'iso8859_3':    'ISO8859_3',
    'iso8859_4':    'ISO8859_4',
    'iso8859_5':    'ISO8859_5',
    'iso8859_6':    'ISO8859_6',
    'iso8859_7':    'ISO8859_7',
    'iso8859_8':    'ISO8859_8',
    'iso8859_9':    'ISO8859_9',
    'iso8859_13':   'ISO8859_13',
    'euc_kr':       'KSC_5601',
    'cp852':        'DOS852',
    'cp857':        'DOS857',
    'cp861':        'DOS861',
    'cp866':        'DOS866',
    'cp869':        'DOS869',
    'cp1250':       'WIN1250',
    'cp1251':       'WIN1251',
    'cp1252':       'WIN1252',
    'cp1253':       'WIN1253',
    'cp1254':       'WIN1254',
    'big5':         'BIG_5',
    'gb2312':       'GB_2312',
    'cp1255':       'WIN1255',
    'cp1256':       'WIN1256',
    'cp1257':       'WIN1257',
    'koi8_r':       'KOI8-R',
    'koi8_u':       'KOI8-U',
    'cp1258':       'WIN1258'
  }

# TODO: Put ALL reserved words here
FIREBIRD_SQL_RESERVED_WORDS = set([
    'SELECT', 'UPDATE', 'INSERT', 'INTO', 'VALUES', 'DELETE', 'EXECUTE', 
    'ALTER', 'CREATE', 'DOMAIN', 'PROCEDURE', 'FROM', 'AFTER', 'AS', 'TRIGGER',
    'TABLE',  'CONSTRAINT', 'INDEX', 'FOREIGN', 'KEY', 'UNIQUE', 'PRIMARY',
    'NOT', 'NULL', 'IN', 'VALUE', 'CHECK', 'DECLARE', 'EXTERNAL', 'FUNCTION',
    'IS', 'RAND', 'SUBSTR', 'DOUBLE', 'PRECISION', 'CHARACTER', 'SET',
    'VARCHAR', 'CHAR', 'BLOB', 'SUBTYPE', 'INTEGER', 'SMALLINT', 'TIMESTAMP', 'DATE',
    'ENTRY_POINT', 'SUB_TYPE', 'UTF8', 'UNICODE_FSS', 'RETURNS', 'REFERENCES',
    'CASCADE', 'ON'
])

for word in PYTHON_TO_FB_ENCODING_MAP.itervalues():
    FIREBIRD_SQL_RESERVED_WORDS.add(word)

def get_data_size(data_type, max_length=100, char_bytes=None):
    from django.db import connection
    if char_bytes is None:
        char_bytes = connection.BYTES_PER_DEFAULT_CHAR
    size_map = {
        'AutoField':                     8,
        'BooleanField':                  4,
        'CharField':                     char_bytes*max_length,
        'CommaSeparatedIntegerField':    max_length,
        'DateField':                     16,
        'DateTimeField':                 16,
        'DecimalField':                  16,
        'FileField':                     char_bytes * max_length,
        'FilePathField':                 'varchar(%(max_length)s)',
        'FloatField':                    16,
        'ImageField':                    char_bytes*max_length,
        'IntegerField':                  8,
        'IPAddressField':                15,
        'NullBooleanField':              4, 
        'OneToOneField':                 8,
        'PhoneNumberField':              20, 
        'PositiveIntegerField':          8,
        'PositiveSmallIntegerField':     4,
        'SlugField':                     char_bytes * max_length,
        'SmallIntegerField':             4,
        'TextBlob':                      8,
        'TextField':                     32767,
        'TimeField':                     16,
        'URLField':                      max_length,
        'USStateField':                  char_bytes * 2
    }
    return size_map[data_type]

def validate_rowsize(opts):
    from django.db import connection
    from django.db.models.fields import FieldDoesNotExist
    errs = set()
    row_size = 0
    columns = []
    for f in opts.local_fields:
        try:
            db_type = f.db_type().strip('"')
        except:
            db_type = 'integer'
        columns.append((db_type, f.get_internal_type(), f.max_length, f.encoding))
    columns_simple = [col[0] for col in columns] 
    text_field_type = '"TextField"'
    max_allowed_bytes = 32765
    if 'TextField' in columns_simple:
        max_length = 100
        num_text_fields = 0
        text_columns = []
        for column in columns:
            if column[0] == 'TextField':
                num_text_fields += 1
                text_columns.append(column)
            if column[0].startswith('varchar') or (column[0] == 'TextField' and column[2]):
                max_length = column[2]
            if column[1] in DATA_TYPES:
                if column[3]:
                    charbytes = 1
                else:
                    charbytes = connection.BYTES_PER_DEFAULT_CHAR
                coltype = column[1]
                if coltype == 'TextField' and column[2]:
                    # Calculate the right size for TextFields with custom max_length
                    coltype = 'CharField'
                row_size += get_data_size(coltype, max_length, charbytes)
        if row_size > 65536:
            max_allowed_bytes = int((max_allowed_bytes/num_text_fields) - (row_size - 65536))
            n = max_allowed_bytes / connection.BYTES_PER_DEFAULT_CHAR
            if n > 512:
                text_field_type = 'varchar(%s)' % n
                errs.add("Row size limit in %s: Maximum number of characters in TextFields will be automatically changed to %s."
                               % (opts.db_table, n))
            else:
                errs.add("Row size limit in %s: Field type will be automatically changed to BLOB."
                               % opts.db_table)
                # Swich to blobs if size is too small (<512)    
                text_field_type = 'blob sub_type text'
    return errs, text_field_type, max_allowed_bytes

def validate_index_limit(f, col_type, opts):
    # More info: http://www.volny.cz/iprenosil/interbase/ip_ib_indexcalculator.htm

    from django.db import connection

    fb_version = "%s.%s" % (connection.ops.firebird_version[0], connection.ops.firebird_version[1])
    page_size = connection.ops.page_size
    
    errs = set()
    
    if connection.ops.index_limit < 1000:
        strip2ascii = False
        custom_charset = False
        if col_type.startswith('varchar'):
            if (f.unique or f.primary_key or f.db_index):
                length = f.max_length
                if not length:
                    try:
                        length = f.rel.to._meta.pk.max_length
                    except AttributeError:
                        pass
                if f.encoding:
                    if not f.encoding.upper().startswith('UTF'):
                        custom_charset = True
                if not custom_charset:
                    try:
                        flength = length * connection.BYTES_PER_DEFAULT_CHAR 
                        if flength >= connection.ops.index_limit:
                            strip2ascii = True
                    except TypeError:
                        pass

            if len(opts.unique_together) > 0:
                if f.column in opts.unique_together[0]:
                    num_unique_char_fields = len([ fld for fld in opts.unique_together[0] if opts.get_field(fld).db_type().startswith('varchar') ])
                    num_unique_fields = len(opts.unique_together[0])
                    num_unique_nonchar_fields = num_unique_fields - num_unique_char_fields
                    limit = connection.ops.index_limit
                    limit -= (num_unique_fields-1) * 52
                    limit -= 8 * num_unique_nonchar_fields
                    max_length = limit/num_unique_char_fields
                    ascii_length = int(f.max_length)
                    if not f.encoding:
                        old_length = ascii_length*connection.BYTES_PER_DEFAULT_CHAR
                    else:
                        old_length = ascii_length

                    if (old_length > max_length) and (ascii_length < max_length) and not f.encoding:
                        strip2ascii = True
                    elif old_length > max_length:
                        strip2ascii = False
                        f.max_length = max_length
                        if not f.encoding:
                            f.encoding = 'ascii'
                        msg =  "Index limit: Character set of the '%s' field (table %s) "
                        msg += "will be automatically changed to ASCII encoding to fit %s-byte limit in FB %s"
                        if not f.encoding:
                            if not page_size:
                                errs.add(msg % (f.column, opts.db_table, connection.ops.index_limit, fb_version))
                            else:
                                msg += " with page size %s"
                                errs.add(msg % (f.column, opts.db_table, connection.ops.index_limit, fb_version, page_size))
                        if max_length != ascii_length:
                            errs.add("Index limit: the maximum length of '%s' is %s instead of %s"
                                % (f.column, max_length, ascii_length))
        if strip2ascii:
            f.encoding = 'ascii'
            msg =  "Index limit: Character set of the '%s' field (table %s) "
            msg += "will be automatically changed to ASCII to fit %s-byte limit in FB %s"
            if not page_size:
                errs.add(msg % (f.column, opts.db_table, connection.ops.index_limit, fb_version))
            else:
                msg += " with page size %s"
                errs.add(msg % (f.column, opts.db_table, connection.ops.index_limit, fb_version, page_size))

    return errs, col_type

def sql_model_create(model, style, known_models=set()):
    """
    Returns the SQL required to create a single model, as a tuple of:
        (list_of_sql, pending_references_dict)
    """
    from django.db import connection, models

    opts = model._meta
    final_output = []
    table_output = []
    pending_references = {}
    qn = connection.ops.quote_name
    
    # Create domains
    domains = [ ('BooleanField', 'smallint CHECK (VALUE IN (0,1))'),
                ('NullBooleanField', 'smallint CHECK ((VALUE IN (0,1)) OR (VALUE IS NULL))'),
                ('PositiveIntegerField', 'integer CHECK ((VALUE >= 0) OR (VALUE IS NULL))'),
                ('PositiveSmallIntegerField', 'smallint CHECK ((VALUE >= 0) OR (VALUE IS NULL))'),
                ('TextField', 'varchar(%s)' % connection.FB_MAX_VARCHAR) ]
    
    connected = True
    try:
        cursor = connection.cursor()
    except:
        connected = False
    if connected:
        cursor.execute("SELECT RDB$FIELD_NAME FROM RDB$FIELDS")
        existing_domains = set([row[0].strip() for row in cursor.fetchall() if not row[0].startswith('RDB$')])
        domains = map(lambda domain: '%s "%s" AS %s;' % ('CREATE DOMAIN', domain[0], domain[1]), 
            filter(lambda x: x[0] not in existing_domains, domains))
        final_output.extend(domains)

    # Check that row size is less than 64k and adjust TextFields if needed
    errs, text_field_type, max_allowed_bytes = validate_rowsize(opts)   
    
    # Create tables
    for f in opts.local_fields:
        col_type = f.db_type()
        if col_type.strip('"') == 'TextField':
            col_type = text_field_type

        errs, col_type = validate_index_limit(f, col_type, opts)

        if (col_type.startswith('varchar') or col_type.strip('"') == 'TextField') and f.encoding:
            charset = PYTHON_TO_FB_ENCODING_MAP[codecs.lookup(f.encoding).name]
            if f.max_length and f.max_length < max_allowed_bytes:
                max_allowed_bytes = f.max_length
            col_type = 'varchar(%i)' % max_allowed_bytes
            col_type = "%s %s %s" % (col_type, "CHARACTER SET", charset)

        if not f.encoding:
            max_allowed_length = connection.BYTES_PER_DEFAULT_CHAR * max_allowed_bytes
            if col_type.strip('"') == 'TextField' and f.max_length and f.max_length < max_allowed_length:
                col_type = 'varchar(%i)' % f.max_length

        if col_type is None:
            # Skip ManyToManyFields, because they're not represented as
            # database columns in this table.
            continue
        else:
            # Make the definition (e.g. 'foo VARCHAR(30)') for this field.
            field_output = [style.SQL_FIELD(qn(f.column)),
                style.SQL_COLTYPE(col_type)]
            field_output.append(style.SQL_KEYWORD('%s' % (not f.null and 'NOT NULL' or '')))
        if f.unique and not f.primary_key:
            field_output.append(style.SQL_KEYWORD('UNIQUE'))
        if f.primary_key:
            field_output.append(style.SQL_KEYWORD('PRIMARY KEY'))
        if f.rel:
            # We haven't yet created the table to which this field
            # is related, so save it for later.
            pr = pending_references.setdefault(f.rel.to, []).append((model, f))
        table_output.append(' '.join(field_output))
    if opts.order_with_respect_to:
        table_output.append(style.SQL_FIELD(qn('_order')) + ' ' + \
            style.SQL_COLTYPE(models.IntegerField().db_type()))
    for field_constraints in opts.unique_together:
        table_output.append(style.SQL_KEYWORD('UNIQUE') + ' (%s)' % \
            ", ".join([qn(style.SQL_FIELD(opts.get_field(f).column)) for f in field_constraints]))

    full_statement = [style.SQL_KEYWORD('CREATE TABLE') + ' ' + style.SQL_TABLE(qn(opts.db_table)) + ' (']
    for i, line in enumerate(table_output): # Combine and add commas.
        full_statement.append('    %s%s' % (line, i < len(table_output)-1 and ',' or ''))
    full_statement.append(');')
    final_output.append('\n'.join(full_statement))

    if opts.has_auto_field:
        # Add any extra SQL needed to support auto-incrementing primary keys.
        auto_column = opts.auto_field.db_column or opts.auto_field.name
        autoinc_sql = connection.ops.autoinc_sql(opts.db_table, auto_column)
        if autoinc_sql:
            for stmt in autoinc_sql:
                final_output.append(stmt)

    # Declare exteral functions
    if connected:
        cursor.execute("SELECT RDB$FUNCTION_NAME FROM RDB$FUNCTIONS")
        existing_functions = set([row[0].strip().upper() for row in cursor.fetchall()])
        if 'RAND' not in existing_functions:
            final_output.append('%s %s\n\t%s %s\n\t%s %s\n\t%s;' % (style.SQL_KEYWORD('DECLARE EXTERNAL FUNCTION'),
                style.SQL_TABLE('RAND'), style.SQL_KEYWORD('RETURNS'), style.SQL_COLTYPE('DOUBLE PRECISION'),
                style.SQL_KEYWORD('BY VALUE ENTRY_POINT'), style.SQL_FIELD("'IB_UDF_rand'"), 
                style.SQL_TABLE("MODULE_NAME 'ib_udf'")))
        if 'SUBSTR' not in existing_functions:
            final_output.append("""DECLARE EXTERNAL FUNCTION SUBSTR CSTRING(255), SMALLINT, SMALLINT
                                   RETURNS CSTRING(255) FREE_IT
                                   ENTRY_POINT 'IB_UDF_substr' MODULE_NAME 'ib_udf';""")

    # Create stored procedures
    if hasattr(model, 'procedures'):
        for proc in model.procedures:
            final_output.append(proc.create_procedure_sql())

    # Create triggers
    if hasattr(model, 'triggers'):
        for proc in model.triggers:
            final_output.append(proc.create_trigger_sql())   
    
    return final_output, pending_references

def many_to_many_sql_for_model(model, style):
    from django.db import connection, models
    from django.contrib.contenttypes import generic
    from django.db.backends.util import truncate_name

    opts = model._meta
    final_output = []
    qn = connection.ops.quote_name
    for f in opts.local_many_to_many:
        if not isinstance(f.rel, generic.GenericRel):
            table_output = [style.SQL_KEYWORD('CREATE TABLE') + ' ' + \
                style.SQL_TABLE(qn(f.m2m_db_table())) + ' (']
            table_output.append('    %s %s %s,' %
                (style.SQL_FIELD(qn('id')),
                style.SQL_COLTYPE(models.AutoField(primary_key=True).db_type()),
                style.SQL_KEYWORD('NOT NULL PRIMARY KEY')))

            table_output.append('    %s %s %s,' %
                (style.SQL_FIELD(qn(f.m2m_column_name())),
                style.SQL_COLTYPE(models.ForeignKey(model).db_type()),
                style.SQL_KEYWORD('NOT NULL')))
            table_output.append('    %s %s %s,' %
                (style.SQL_FIELD(qn(f.m2m_reverse_name())),
                style.SQL_COLTYPE(models.ForeignKey(f.rel.to).db_type()),
                style.SQL_KEYWORD('NOT NULL')))
            deferred = [
                (f.m2m_db_table(), f.m2m_column_name(), opts.db_table,
                    opts.pk.column),
                ( f.m2m_db_table(), f.m2m_reverse_name(),
                    f.rel.to._meta.db_table, f.rel.to._meta.pk.column)
                ]

            table_output.append('    %s (%s, %s)' %
                (style.SQL_KEYWORD('UNIQUE'),
                style.SQL_FIELD(qn(f.m2m_column_name())),
                style.SQL_FIELD(qn(f.m2m_reverse_name()))))
            table_output.append(');')
            final_output.append('\n'.join(table_output))

            autoinc_sql = connection.ops.autoinc_sql(f.m2m_db_table(), 'id')
            if autoinc_sql:
                for stmt in autoinc_sql:
                    final_output.append(stmt)

            if TEST_MODE < 2:
                for r_table, r_col, table, col in deferred:
                    r_name = connection.ops.reference_name(r_col, col, r_table, table)
                    final_output.append(style.SQL_KEYWORD('ALTER TABLE') + ' %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s;' % 
                    (qn(r_table),
                    truncate_name(r_name, connection.ops.max_name_length()),
                    qn(r_col), qn(table), qn(col),
                    'ON DELETE CASCADE ON UPDATE CASCADE'))

    return final_output

def custom_sql_for_model(model):
    from django.db import models, connection
    from django.conf import settings

    qn = connection.ops.quote_name
    opts = model._meta
    app_dir = os.path.normpath(os.path.join(os.path.dirname(models.get_app(model._meta.app_label).__file__), 'sql'))
    output = []

    # Some backends can't execute more than one SQL statement at a time,
    # so split into separate statements.
    statements = re.compile(r";[ \t]*$", re.M)

    # Find custom SQL, if it's available.
    sql_files = [os.path.join(app_dir, "%s.%s.sql" % (opts.object_name.lower(), settings.DATABASE_ENGINE)),
                 os.path.join(app_dir, "%s.sql" % opts.object_name.lower())]
    for sql_file in sql_files:
        if os.path.exists(sql_file):
            fp = open(sql_file, 'U')
            for statement in statements.split(fp.read().decode(settings.FILE_CHARSET)):
                # Remove any comments from the file
                statement = re.sub(ur"--.*[\n\Z]", "", statement)
                if statement.strip():
                    new_statement = []
                    for word in statement.split():
                        if word.isalpha() and word.strip().upper() not in FIREBIRD_SQL_RESERVED_WORDS:
                            new_statement.append(qn(word))
                            continue
                        new_statement.append(word)
                    statement = ' '.join(new_statement)
                    output.append(statement + u";")
            fp.close()

    return output

TEST_DATABASE_PREFIX = 'test_'
def create_test_db(settings, connection, verbosity, autoclobber):
    # KInterbasDB supports dynamic database creation and deletion 
    # via the module-level function create_database and the method Connection.drop_database.
       
    if settings.TEST_DATABASE_NAME:
        TEST_DATABASE_NAME = settings.TEST_DATABASE_NAME
    else:
        dbnametuple = os.path.split(settings.DATABASE_NAME)
        TEST_DATABASE_NAME = os.path.join(dbnametuple[0], TEST_DATABASE_PREFIX + dbnametuple[1])

    dsn = "localhost:%s" % TEST_DATABASE_NAME
    if settings.DATABASE_HOST:
        dsn = "%s:%s" % (settings.DATABASE_HOST, TEST_DATABASE_NAME)

    if os.path.isfile(TEST_DATABASE_NAME):
        sys.stderr.write("Database %s already exists\n" % TEST_DATABASE_NAME)
        if not autoclobber:
            confirm = raw_input("Type 'yes' if you would like to try deleting the test database '%s', or 'no' to cancel: " % TEST_DATABASE_NAME)
        if autoclobber or confirm == 'yes':
            if verbosity >= 1:
                print "Destroying old test database..."
            old_connection = connect(dsn=dsn, user=settings.DATABASE_USER, password=settings.DATABASE_PASSWORD)
            old_connection.drop_database()
        else:
                print "Tests cancelled."
                sys.exit(1)

    if verbosity >= 1:
        print "Creating test database..."
    try:
        charset = hasattr(settings, 'FIREBIRD_CHARSET') \
                  and settings.FIREBIRD_CHARSET in ('UNICODE_FSS', 'UTF8') \
                  and settings.FIREBIRD_CHARSET or 'UNICODE_FSS'
        if hasattr(settings, 'FIREBIRD_CHARSET'):
            if settings.FIREBIRD_CHARSET == 'UTF8':
                charset='UTF8'                
        create_database("create database '%s' user '%s' password '%s' default character set %s" % \
            (dsn, settings.DATABASE_USER, settings.DATABASE_PASSWORD, charset))
    except Exception, e:
        sys.stderr.write("Got an error creating the test database: %s\n" % e)
        sys.exit(2)

    connection.close() 
    settings.DATABASE_NAME = TEST_DATABASE_NAME

    call_command('syncdb', verbosity=verbosity, interactive=False)

    if settings.CACHE_BACKEND.startswith('db://'):
        cache_name = settings.CACHE_BACKEND[len('db://'):]
        call_command('createcachetable', cache_name)

    # Get a cursor (even though we don't need one yet). This has
    # the side effect of initializing the test database.
    cursor = connection.cursor()

    return TEST_DATABASE_NAME

def destroy_test_db(settings, connection, old_database_name, verbosity):
    # KInterbasDB supports dynamic database deletion via the method Connection.drop_database.
    if verbosity >= 1:
        print "Destroying test database..."
    connection.drop_database()
    

