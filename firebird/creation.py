import re
from django.conf import settings
from django.db.backends.creation import BaseDatabaseCreation

precision_re = re.compile(r'\((\d{1,2}), (\d{1,2})\)')

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
            
        This method takes the result from BaseDatabaseCreation.sql_create_model(),
        and makes some Firebird specific changes to sql.
        """
        
        def precision_replace(matchobj):
            precision = matchobj.group(1)
            scale = matchobj.group(2)             
            if precision > 18:
                precision = 18
            if scale > 18:
                scale = 18
            return '('+str(precision)+', '+str(scale)+')'
        
        final_output, pending_references = super(DatabaseCreation, self).sql_create_model(model=model, style=style, known_models=known_models)
        
        output_parts = []
        for part in final_output:
            output = ''
            for line in part.splitlines(True):
                # Precision and scale should be in range from 1 to 18
                # http://ibexpert.net/ibe/index.php?n=Doc.FieldDefinitions#NUMERIC
                if 'numeric(' in line:
                    line = re.sub(precision_re, precision_replace, line)
                # NOT NULL keyword should be located after field constraint definition
                if '%' in line:
                    if 'NOT NULL' in line:
                        line = line.replace('NOT NULL', '').replace('%', 'NOT NULL')
                    else:
                        line = line.replace('%', '')
                output += line
            output_parts.append(output)
                 
        return output_parts, pending_references
    
    def _create_test_db(self, verbosity, autoclobber):
        test_database_name = self.connection.settings_dict['TEST_NAME']
        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        pass