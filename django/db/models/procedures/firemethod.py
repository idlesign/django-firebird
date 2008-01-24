# Stored procedures' Django model method wrapper
#
# Works only with Python 2.5+
# For Python < 2.5 use class notation
#
#    Example:
#
#    @firemethod    
#    def was_published_today_from_id(self_id='integer'):
#        """
#        SELECT "id"
#        FROM "firemethod_article"
#        WHERE "pub_date" = 'today' AND "id" = :self_id
#        INTO :result;
#        IF (result IS NULL) THEN
#            result = 0;
#        ELSE
#            result = 1;    
#        EXIT;
#        """
#        return ('result', 'integer')
#   
#    @firemethod    
#    def was_published_today_filter():
#        """
#        FOR SELECT "id", "headline", "pub_date"
#        FROM "firemethod_article"
#        WHERE "pub_date" = 'today'
#        INTO :other_id, :other_headline, :other_pub_date
#        DO BEGIN SUSPEND; END
#        """
#        # Dummy variable just to show how to declare
#        declare = ('dummy', 'varchar(200)')
#        return ('other_id', 'integer'), ('other_headline', 'varchar(100)'), ('other_pub_date', 'date')
#   
#   procedures = (was_published_today_filter, was_published_today_from_id)
#
#
# >>> article.was_published_today_from_id(article.pk)
# 1
# >>> bool(art.was_published_today_from_id(other_article.pk))
# False
# >>>Article.objects.firefilter(self.was_published_today_filter)
# [<Article: Only for today>, <Article: The life is always in a current moment>]

from functools import *
from django.db.models import Procedure
from django.db.models.procedures import procedure_meta

def firemethod(f):
    @wraps(f)
    def wrapper():
        params, returns, vars, ret = None, None, None, f()
        attrs = {}
        if f.func_code.co_varnames:
            all_vars = list(f.func_code.co_varnames)
            if 'declare' in all_vars:
                all_tuples = set([ d for d in f.func_code.co_consts if isinstance(d, tuple) ])
                if isinstance(ret[0], basestring):
                    ret = [ret]
                ret_tuples = set(ret)
                all_vars.remove('declare')
                vars = []
                for var, var_type in all_tuples - ret_tuples:
                    vars.append((var, var_type))
            if len(all_vars) > 0 and f.func_defaults:
                params = []
                for key, value in zip(all_vars, f.func_defaults) :
                     params.append((key, value))
        if ret:
            returns = []
            if isinstance(ret[0], basestring):
                    ret = [ret]
            for var, var_type in ret:
                returns.append((var, var_type))         
        if params:
            attrs['__params__'] = params
        if returns:
            attrs['__returns__'] = returns
        if vars:
            attrs['__vars__'] = vars
        attrs['__body__'] = f.__doc__
        return procedure_meta(f.__name__, (Procedure,), attrs)
    return wrapper()
        
