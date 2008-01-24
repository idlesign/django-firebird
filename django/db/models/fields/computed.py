from django.db.models.fields import CharField, DateField, DateTimeField, DecimalField
from django.db.models.fields import IntegerField, FloatField, SmallIntegerField, TimeField

class ComputedField(object):
    def __init__(self, **kwargs):
        expression = kwargs.pop('expression')
        if 'params' in kwargs:
            raw_params = kwargs.pop('params')
            params = []
            for rp in raw_params:
                params.append('"%s"' % rp)
            self.expression = expression % tuple(params)
        else:
            self.expression = expression    
    def db_type(self):
        return 'ComputedField'

class ComputedCharField(ComputedField, CharField):
    def __init__(self, **kwargs):
        ComputedField.__init__(self, **kwargs)
        if 'params' in kwargs:
            del kwargs['params']
        if 'expression' in kwargs:
            del kwargs['expression']
        CharField.__init__(self, **kwargs)

class ComputedDateField(ComputedField, DateField):
    def __init__(self, **kwargs):
        ComputedField.__init__(self, **kwargs)
        if 'params' in kwargs:
            del kwargs['params']
        if 'expression' in kwargs:
            del kwargs['expression']
        DateField.__init__(self, **kwargs)  

class ComputedDateTimeField(ComputedField, DateTimeField):
    def __init__(self, **kwargs):
        ComputedField.__init__(self, **kwargs)
        if 'params' in kwargs:
            del kwargs['params']
        if 'expression' in kwargs:
            del kwargs['expression'] 
        DateTimeField.__init__(self, **kwargs) 

class ComputedDecimalField(ComputedField, DecimalField):
    def __init__(self, **kwargs):
        ComputedField.__init__(self, **kwargs)
        if 'params' in kwargs:
            del kwargs['params']
        if 'expression' in kwargs:
            del kwargs['expression']
        DecimalField.__init__(self, **kwargs)
        
class ComputedFloatField(ComputedField, FloatField):
    def __init__(self, **kwargs):
        ComputedField.__init__(self, **kwargs)
        if 'params' in kwargs:
            del kwargs['params']
        if 'expression' in kwargs:
            del kwargs['expression']
        FloatField.__init__(self, **kwargs)

class ComputedIntegerField(ComputedField, IntegerField):
    def __init__(self, **kwargs):
        ComputedField.__init__(self, **kwargs)
        if 'params' in kwargs:
            del kwargs['params']
        if 'expression' in kwargs:
            del kwargs['expression']
        IntegerField.__init__(self, **kwargs)

class ComputedSmallIntegerField(ComputedField, SmallIntegerField):
    def __init__(self, **kwargs):
        ComputedField.__init__(self, **kwargs)
        if 'params' in kwargs:
            del kwargs['params']
        if 'expression' in kwargs:
            del kwargs['expression']
        SmallIntegerField.__init__(self, **kwargs)

class ComputedTimeField(ComputedField, TimeField):
    def __init__(self, **kwargs):
        ComputedField.__init__(self, **kwargs)
        if 'params' in kwargs:
            del kwargs['params']
        if 'expression' in kwargs:
            del kwargs['expression'] 
        TimeField.__init__(self, **kwargs)    

