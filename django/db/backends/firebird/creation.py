# This dictionary maps Field objects to their associated Firebird column
# types, as strings. Column-type strings can contain format strings; they'll
# be interpolated against the values of Field.__dict__ before being output.
# If a column type is set to None, it won't be included in the output.


from kinterbasdb import connect, create_database
from django.core.management import call_command
from django.conf import settings
from django.db import connection
import sys, os, os.path, codecs
try:
    set
except NameError:
    from sets import Set as set   # Python 2.3 fallback

# Setting TEST_MODE to 1 enables cascading deletes (for table flush) which are dangerous
# Setting TEST_MODE to 2 disables strict FK constraints (for forward/post references)
# Setting TEST_MODE to 0 is the most secure option (it even fails some official Django tests  because of it)
TEST_MODE = 0
if 'FB_DJANGO_TEST_MODE' in os.environ:
    TEST_MODE = int(os.environ['FB_DJANGO_TEST_MODE'])

DATA_TYPES = {
    'AutoField':                     '"AutoField"',
    'BooleanField':                  '"BooleanField"',
    'CharField':                     'varchar(%(max_length)s)',
    'CommaSeparatedIntegerField':    'varchar(%(max_length)s) CHARACTER SET ASCII',
    'DateField':                     '"DateField"',
    'DateTimeField':                 '"DateTimeField"',
    'DecimalField':                  'numeric(%(max_digits)s, %(decimal_places)s)',
    'DefaultCharField':              '"CharField"',
    'FileField':                     'varchar(%(max_length)s)',
    'FilePathField':                 'varchar(%(max_length)s)',
    'FloatField':                    '"FloatField"',
    'ImageField':                    '"varchar(%(max_length)s)"',
    'IntegerField':                  '"IntegerField"',
    'IPAddressField':                'varchar(15) CHARACTER SET ASCII',
    'NullBooleanField':              '"NullBooleanField"', 
    'OneToOneField':                 '"OneToOneField"',
    'PhoneNumberField':              '"PhoneNumberField"', 
    'PositiveIntegerField':          '"PositiveIntegerField"',
    'PositiveSmallIntegerField':     '"PositiveSmallIntegerField"',
    'SlugField':                     'varchar(%(max_length)s)',
    'SmallIntegerField':             '"SmallIntegerField"',
    'LargeTextField':                '"LargeTextField"',
    'TextField':                     '"TextField"',
    'TimeField':                     '"TimeField"',
    'URLField':                      'varchar(%(max_length)s) CHARACTER SET ASCII',
    'USStateField':                  '"USStateField"'
}
      
PYTHON_TO_FB_ENCODING_MAP = {
    'ascii':        'ASCII',
    'utf_8':        connection.charset,
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

def get_data_size(data_type, max_length = 100):
    char_bytes = connection.BYTES_PER_DEFAULT_CHAR
    size_map = {
        'AutoField':                     8,
        'BooleanField':                  4,
        'CharField':                     char_bytes*max_length,
        'CommaSeparatedIntegerField':    max_length,
        'DateField':                     16,
        'DateTimeField':                 16,
        'DecimalField':                  16,
        'FileField':                     char_bytes*max_length,
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
        'SlugField':                     char_bytes*max_length,
        'SmallIntegerField':             4,
        'TextBlob':                      8,
        'TextField':                     32767,
        'TimeField':                     16,
        'URLField':                      max_length,
        'USStateField':                  char_bytes*2
    }
    return size_map[data_type]

DEFAULT_MAX_LENGTH = 100
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
    domains = [ ('AutoField', 'integer'),
                ('BooleanField', 'smallint CHECK (VALUE IN (0,1))'),
                ('DateField', 'date'),
                ('CharField', 'varchar(%i)' % DEFAULT_MAX_LENGTH),
                ('DateTimeField', 'timestamp'),
                ('FloatField', 'double precision'),
                ('IntegerField', 'integer'),
                ('IPAddressField', 'varchar(15) CHARACTER SET ASCII'),
                ('NullBooleanField', 'smallint CHECK ((VALUE IN (0,1)) OR (VALUE IS NULL))'),
                ('OneToOneField', 'integer'),
                ('PhoneNumberField', 'varchar(20) CHARACTER SET ASCII'),
                ('PositiveIntegerField', 'integer CHECK ((VALUE >= 0) OR (VALUE IS NULL))'),
                ('PositiveSmallIntegerField', 'smallint CHECK ((VALUE >= 0) OR (VALUE IS NULL))'),
                ('SmallIntegerField', 'smallint'),
                ('TextField', 'varchar(%s)' % connection.FB_MAX_VARCHAR),
                ('LargeTextField', 'blob sub_type text'),
                ('TimeField', 'time'),
                ('USStateField', 'varchar(2) CHARACTER SET ASCII') ]
    
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
    row_size = 0
    columns = [(f.db_type().strip('"'), f.get_internal_type(), f) for f in opts.fields]
    columns_simple = [col[0] for col in columns] 
    text_field_type = '"TextField"'
    max_alowed_bytes = 32765
    if 'TextField' in columns_simple:
        max_length = 100
        num_text_fields = 0
        for column in columns:
            num_text_fields += (column[0] == 'TextField')
            if column[0].startswith('varchar'):
                max_length = int(column[0].split('(')[1].split(')')[0])
            if column[1] in DATA_TYPES:
                row_size += get_data_size(column[1], max_length)
        if row_size > 65536:
            max_alowed_bytes = int( (max_alowed_bytes/num_text_fields) - (row_size-65536) )
            n = max_alowed_bytes / connection.BYTES_PER_DEFAULT_CHAR
            if n > 512: 
                text_field_type = 'varchar(%s)' % n
                FB_TEXTFIELD_ALTERED = True    
                print
                print "WARNING: Maximum number of characters in TextFields has changed to %s." % n
                print "         TextField columns with custom charsets will have %s chars available" % max_alowed_bytes 
                print "         The change affects %s table only." % opts.db_table
                print "         TextFields in other tables will have %s characters maximum" % connection.FB_MAX_VARCHAR
                print "         or 32765 characters with custom (non-UTF) encoding."
                print "         If you need more space in those fields try LargeTextFields instead." 
                print
            else:
                # Swich to blobs if size is too small (<1024)    
                text_field_type = '"LargeTextField"'    
    
    # Create tables
    for f in opts.fields:
        col_type = f.db_type()
        if col_type.strip('"') == 'TextField':
            col_type = text_field_type
        fb_version = "%s.%s" % (connection.ops.firebird_version[0], connection.ops.firebird_version[1])
        page_size = connection.ops.page_size
        #look at: http://www.volny.cz/iprenosil/interbase/ip_ib_indexcalculator.htm
        if connection.ops.index_limit < 1000:
            strip2ascii = False
            custom_charset = False
            if col_type.startswith('varchar'):
                if (f.unique or f.primary_key or f.db_index):
                    length = f.max_length
                    if f.encoding:
                        if not f.encoding.upper().startswith('UTF'):
                            custom_charset = True
                    if not custom_charset:
                        try:
                            length = f.max_length * connection.BYTES_PER_DEFAULT_CHAR 
                        except TypeError:
                            length = 100*connection.BYTES_PER_DEFAULT_CHAR #Default for CharField
                    if length >= connection.ops.index_limit:   
                        strip2ascii = True
                if len(opts.unique_together) > 0:
                    if f.column in opts.unique_together[0]:
                        num_unique_char_fields = len([ fld for fld in opts.unique_together[0] if opts.get_field(fld).db_type().startswith('varchar') ])
                        num_unique_fileds = len(opts.unique_together[0])
                        num_unique_nonchar_fileds = num_unique_fileds - num_unique_char_fields
                        limit = connection.ops.index_limit
                        limit -= ((num_unique_fileds - 1)*64)
                        limit -= 8*num_unique_nonchar_fileds
                        max_length = limit/num_unique_char_fields
                        ascii_length = int(f.max_length)
                        old_length = ascii_length*connection.BYTES_PER_DEFAULT_CHAR
                         
                        if (old_length >= max_length) and (ascii_length < max_length):
                            strip2ascii = True
                        elif old_length > max_length:
                            strip2ascii = False #We change it here
                            col_type = "varchar(%i) CHARACTER SET ASCII" % max_length
                            msg =  "WARNING: Character set of the '%s' field\n"
                            msg += "         (table %s)\n"
                            msg += "         has changed to ASCII"
                            msg += " to fit %s-byte limit in FB %s"
                            if not page_size:
                                print  msg % (f.column, opts.db_table, connection.ops.index_limit, fb_version)
                            else:
                                msg += " with page size %s"
                                print  msg % (f.column, opts.db_table, connection.ops.index_limit, fb_version, page_size)
                            print "         The maximum length of '%s' is now %s instead of %s"\
                             % (f.column, max_length, old_length)     
            if strip2ascii:
                col_type = "%s %s %s" % (col_type, "CHARACTER SET", "ASCII")
                msg =  "WARNING: Character set of the '%s' field\n"
                msg += "         (table %s)\n"
                msg += "         has changed to ASCII"
                msg += " to fit %s-byte limit in FB %s"
                if not page_size:
                    print  msg % (f.column, opts.db_table, connection.ops.index_limit, fb_version)
                else:
                    msg += " with page size %s"
                    print  msg % (f.column, opts.db_table, connection.ops.index_limit, fb_version, page_size)
                    
        if (col_type.startswith('varchar') or col_type.strip('"') == 'TextField') and f.encoding:
            charset = PYTHON_TO_FB_ENCODING_MAP[codecs.lookup(f.encoding).name]
            if col_type.strip('"') == 'TextField':
                col_type = 'varchar(%i)' % max_alowed_bytes
            col_type = "%s %s %s" % (col_type, "CHARACTER SET", charset)
       
        if col_type is None:
            # Skip ManyToManyFields, because they're not represented as
            # database columns in this table.
            continue
        if col_type == 'ComputedField':
            # Make the definition (e.g. 'foo COMPUTED BY (oldfoo*2)') for this field.
            field_output = [ style.SQL_FIELD(qn(f.column)), style.SQL_KEYWORD('COMPUTED BY'),
                             '(%s)' % f.expression ] 
                
        else:    
            # Make the definition (e.g. 'foo VARCHAR(30)') for this field.
            field_output = [style.SQL_FIELD(qn(f.column)),
                style.SQL_COLTYPE(col_type)]
            field_output.append(style.SQL_KEYWORD('%s' % (not f.null and 'NOT NULL' or '')))
        if f.unique:
            field_output.append(style.SQL_KEYWORD('UNIQUE'))
        if f.primary_key:
            field_output.append(style.SQL_KEYWORD('PRIMARY KEY'))
        if f.rel:
            # We haven't yet created the table to which this field
            # is related, so save it for later.
            pr = pending_references.setdefault(f.rel.to, []).append((model, f))
        table_output.append(' '.join(field_output))
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
        autoinc_sql = connection.ops.autoinc_sql(style, opts.db_table, auto_column)
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
    for f in opts.many_to_many:
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
            
            autoinc_sql = connection.ops.autoinc_sql(style, f.m2m_db_table(), 'id')
            if autoinc_sql:
                for stmt in autoinc_sql:
                    final_output.append(stmt)
            
            if TEST_MODE < 2:
                for r_table, r_col, table, col in deferred:
                    r_name = '%s_refs_%s_%x' % (r_col, col,
                            abs(hash((r_table, table))))
                    final_output.append(style.SQL_KEYWORD('ALTER TABLE') + ' %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)%s;' % 
                    (qn(r_table),
                    truncate_name(r_name, connection.ops.max_name_length()),
                    qn(r_col), qn(table), qn(col),
                    'ON DELETE CASCADE ON UPDATE CASCADE'))

    return final_output

def sql_for_pending_references(model, style, pending_references):
    """
    Returns any ALTER TABLE statements to add constraints after the fact.
    """
    from django.db import connection
    
    qn = connection.ops.quote_name
    final_output = []
    if TEST_MODE < 2:
        opts = model._meta
        if model in pending_references:
            for rel_class, f in pending_references[model]:
                rel_opts = rel_class._meta
                r_table = rel_opts.db_table
                r_col = f.column
                table = opts.db_table
                col = opts.get_field(f.rel.field_name).column
                r_name = connection.ops.reference_name(r_col, col, r_table, table)
                if not f.on_update:
                    f.on_update = 'CASCADE'
                if not f.on_delete:
                    if TEST_MODE > 0:
                        f.on_delete = 'CASCADE'
                    else:
                        if f.null:
                            f.on_delete = 'SET NULL'
                        else:
                            f.on_delete = 'NO ACTION'
                final_output.append(style.SQL_KEYWORD('ALTER TABLE') + ' %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE %s ON DELETE %s;' % \
                    (qn(r_table), r_name, qn(r_col), qn(table), qn(col),
                    f.on_update, f.on_delete))

            del pending_references[model]
    return final_output


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
        charset = 'UNICODE_FSS'
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
    

