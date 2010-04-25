from django.conf import settings
from django.db.backends.creation import BaseDatabaseCreation

class DatabaseCreation(BaseDatabaseCreation):
    # This dictionary maps Field objects to their associated Firebird column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__ before being output.
    # If a column type is set to None, it won't be included in the output.
    #
    # Any format strings starting with "qn_" are quoted before being used in the
    # output (the "qn_" prefix is stripped before the lookup is performed.

    data_types = {
        'AutoField':         'integer',
        'BooleanField':      'integer',
        'CharField':         'varchar(%(max_length)s)',
        'CommaSeparatedIntegerField': 'varchar(%(max_length)s)',
        'DateField':         'date',
        'DateTimeField':     'timestamp',
        'DecimalField':      'numeric(%(max_digits)s, %(decimal_places)s)',
        'FileField':         'varchar(%(max_length)s)',
        'FilePathField':     'varchar(%(max_length)s)',
        'FloatField':        'double precision',
        'IntegerField':      'integer',
        'IPAddressField':    'char(15)',
        'NullBooleanField':  'integer',
        'OneToOneField':     'integer',
        'PositiveIntegerField': 'integer %% CHECK (%(qn_column)s >= 0)',
        'PositiveSmallIntegerField': 'smallint %% CHECK (%(qn_column)s >= 0)',
        'SlugField':         'varchar(%(max_length)s)',
        'SmallIntegerField': 'smallint',
        'TextField':         'blob sub_type 1',
        'TimeField':         'time',
    }

    def sql_create_model(self, model, style, known_models=set()):
        """
        Returns the SQL required to create a single model, as a tuple of:
            (list_of_sql, pending_references_dict)
        """
        from django.db import models
        opts = model._meta
        if not opts.managed:
            return [], {}
        final_output = []
        table_output = []
        pending_references = {}
        qn = self.connection.ops.quote_name
        for f in opts.local_fields:
            col_type = f.db_type()
            tablespace = f.db_tablespace or opts.db_tablespace
            if col_type is None:
                # Skip ManyToManyFields, because they're not represented as
                # database columns in this table.
                continue
            # Make the definition (e.g. 'foo VARCHAR(30)') for this field.
            field_output = [style.SQL_FIELD(qn(f.column)), style.SQL_COLTYPE(col_type)]          
            if not f.null:
                # Workaraund  for Firebird 1.5 : the NOT NULL keyword should be located after field constraint definition
				# Mark '%' is 'NOT NULL' placeholder
                if field_output[-1].find('%') == -1:
                    field_output.append(style.SQL_KEYWORD('NOT NULL'))
                else:
                    field_output[-1] = field_output[-1].replace('%', style.SQL_KEYWORD('NOT NULL'))
            elif field_output[-1].find('%'):    # Erase placeholder
                field_output[-1] = field_output[-1].replace('%','')
            if f.primary_key:
                field_output.append(style.SQL_KEYWORD('PRIMARY KEY'))
            elif f.unique:
                field_output.append(style.SQL_KEYWORD('UNIQUE'))
            if tablespace and f.unique:
                # We must specify the index tablespace inline, because we
                # won't be generating a CREATE INDEX statement for this field.
                field_output.append(self.connection.ops.tablespace_sql(tablespace, inline=True))
            if f.rel:
                ref_output, pending = self.sql_for_inline_foreign_key_references(f, known_models, style)
                if pending:
                    pr = pending_references.setdefault(f.rel.to, []).append((model, f))
                else:
                    field_output.extend(ref_output)
            table_output.append(' '.join(field_output))
        if opts.order_with_respect_to:
            table_output.append(style.SQL_FIELD(qn('_order')) + ' ' + \
                style.SQL_COLTYPE(models.IntegerField().db_type()))
        for field_constraints in opts.unique_together:
            table_output.append(style.SQL_KEYWORD('UNIQUE') + ' (%s)' % \
                ", ".join([style.SQL_FIELD(qn(opts.get_field(f).column)) for f in field_constraints]))

        full_statement = [style.SQL_KEYWORD('CREATE TABLE') + ' ' + style.SQL_TABLE(qn(opts.db_table)) + ' (']
        for i, line in enumerate(table_output): # Combine and add commas.
            full_statement.append('    %s%s' % (line, i < len(table_output)-1 and ',' or ''))
        full_statement.append(')')
        if opts.db_tablespace:
            full_statement.append(self.connection.ops.tablespace_sql(opts.db_tablespace))
        full_statement.append(';')
        final_output.append('\n'.join(full_statement))

        if opts.has_auto_field:
            # Add any extra SQL needed to support auto-incrementing primary keys.
            auto_column = opts.auto_field.db_column or opts.auto_field.name
            autoinc_sql = self.connection.ops.autoinc_sql(opts.db_table, auto_column)
            if autoinc_sql:
                for stmt in autoinc_sql:
                    final_output.append(stmt)

        return final_output, pending_references
    
    def _create_test_db(self, verbosity, autoclobber):
        test_database_name = self.connection.settings_dict['TEST_NAME']
        if test_database_name and test_database_name != ":memory:":
            # Erase the old test database
            if verbosity >= 1:
                print "Destroying old test database..."
            if os.access(test_database_name, os.F_OK):
                if not autoclobber:
                    confirm = raw_input("Type 'yes' if you would like to try deleting the test database '%s', or 'no' to cancel: " % test_database_name)
                if autoclobber or confirm == 'yes':
                  try:
                      if verbosity >= 1:
                          print "Destroying old test database..."
                      os.remove(test_database_name)
                  except Exception, e:
                      sys.stderr.write("Got an error deleting the old test database: %s\n" % e)
                      sys.exit(2)
                else:
                    print "Tests cancelled."
                    sys.exit(1)
            if verbosity >= 1:
                print "Creating test database..."
        else:
            test_database_name = ":memory:"
        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        if test_database_name and test_database_name != ":memory:":
            # Remove the SQLite database file
            os.remove(test_database_name)