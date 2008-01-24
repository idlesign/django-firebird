from django.db import connection, transaction


class procedure_meta(type):
    """
    Metaclass for native stored procedures behaving as normal Django model methods
    Subclass Procedure inside your model, create procedures tuple
    and use the class as ordinary method
    Please treat the resulting classes as methods because they don't
    behave like classes anymore
    Look at tests/customtests/custom_methods/model.py for demonstration
    """
    def __init__(cls, name, bases, attrs):
        super(procedure_meta, cls).__init__(name, bases, attrs)
        params, returns, vars, body = None, None, None, None
        if '__params__' in attrs:
            params = attrs['__params__']
        if '__returns__' in attrs:
            returns = attrs['__returns__']
        if '__vars__' in attrs:
            vars = attrs['__vars__']
        if name != 'Procedure':
            assert '__body__' in attrs, "Procedure must have body"   
            header = ['CREATE OR ALTER PROCEDURE %s ' % name]
            if params:
                header.append('(')
                header.append(', '.join([param + ' ' + paramtype for param, paramtype in params]))
                header.append(')')
            if returns:
                header.append('\nRETURNS (')
                header.append(', '.join([ret + ' ' + rettype for ret, rettype in returns]))
                header.append(')')   
            cls.__sql__ = [''.join(header)]
            cls.__sql__.append('AS')
            if vars:
                var_declare = ['DECLARE VARIABLE ']
                var_declare.append(', '.join([var + ' ' + vartype for var, vartype in vars]))
                var_declare.append(';')
                cls.__sql__.append(''.join(var_declare))
            cls.__sql__.extend(['BEGIN', attrs['__body__'], 'END'])
            cls.sql = '\n'.join(cls.__sql__)
            cls.params = params
            cls.returns = returns
            cls.procedure_name = name
    
    def create_procedure_sql(cls):
        return cls.sql
    
    def execute(cls, cursor, *args):
        if args:
            return cursor.execute_straight('SELECT %s FROM %s(%s);' %\
                    (', '.join("%s" % ret[0] for ret in cls.returns),
                     cls.procedure_name,
                     ', '.join("?" * len(args))), args)
        else:
            return cursor.execute_straight('SELECT %s FROM %s;' %\
                    (', '.join("%s" % ret[0] for ret in cls.returns),
                     cls.procedure_name))

    def __call__(cls, *args):
        cursor = connection.cursor()
        if not cls.returns:
            cursor.execute('EXECUTE PROCEDURE "%s" %s;' %\
                (cls.procedure_name,
                 ' ,'.join("'%s'" % arg for arg in args)))
        elif len(cls.returns) == 1:
            # Procedure returns value
            cursor.callproc(cls.procedure_name, args)
            return cursor.fetchone()[0]
        
        else:
            cursor.execute_straight('SELECT %s FROM %s(%s);' %\
                (', '.join("%s" % ret[0] for ret in cls.returns),
                 cls.procedure_name,
                 ', '.join("?" * len(args))), args)       
            return cursor.fetchall()

class trigger_meta(type):
    """
    """
    def __init__(cls, name, bases, attrs):
        super(trigger_meta, cls).__init__(name, bases, attrs)
        vars, body, table, mode = None, None, None, None
        if '__vars__' in attrs:
            vars = attrs['__vars__']
        if name != 'Trigger':
            assert '__body__' in attrs, "Triggers must have some SQL code"
            assert '__table__' in attrs, "Triggers must have the table"
            assert '__mode__' in attrs, "Triggers must have a mode, e.g. AFTER INSERT"   
            header = 'CREATE OR ALTER TRIGGER %s FOR %s\n%s AS' %\
             (name, connection.ops.quote_name(attrs['__table__']), attrs['__mode__'])
            cls.__sql__ = [header]
            if vars:
                var_declare = ['DECLARE VARIABLE ']
                var_declare.append(', '.join([var + ' ' + vartype for var, vartype in vars]))
                var_declare.append(';')
                cls.__sql__.append(''.join(var_declare))
            cls.__sql__.extend(['BEGIN', attrs['__body__'], 'END'])
            cls.sql = '\n'.join(cls.__sql__)
            cls.trigger_name = name
    def create_trigger_sql(cls):
        return cls.sql

class Procedure(object):
    __metaclass__ = procedure_meta

class Trigger(object):
    __metaclass__ = trigger_meta

