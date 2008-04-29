# Cache. Maps default query class to new Oracle query class.
_classes = {}

def query_class(QueryClass, Database):
    """
    Returns a custom djang.db.models.sql.query.Query subclass that is
    appropraite for Oracle.

    The 'Database' module (cx_Oracle) is passed in here so that all the setup
    required to import it only needs to be done by the calling module.
    """
    global _classes
    try:
        return _classes[QueryClass]
    except KeyError:
        pass
    
    class FirebirdQuery(QueryClass):
        def as_sql(self, with_limits=True, with_col_aliases=False):
            do_offset = with_limits and (self.high_mark or self.low_mark)
            if not do_offset:
                return super(FirebirdQuery, self).as_sql(with_limits=False, with_col_aliases=with_col_aliases)

            self.pre_sql_setup()
            limit_offset_before = []
            if self.high_mark:
                limit_offset_before.append("FIRST %d" % (self.high_mark - self.low_mark))
            if self.low_mark: 
                limit_offset_before.append("SKIP %d" % self.low_mark)

            sql, params= super(FirebirdQuery, self).as_sql(with_limits=False, with_col_aliases=True)
            result = sql.replace('SELECT', "SELECT %s" % ' '.join(limit_offset_before))
            return result, params
    _classes[QueryClass] = FirebirdQuery
    return FirebirdQuery
