# Cache. Maps default query class to new Firebird query class.
_classes = {}

def query_class(QueryClass):
    """
    Returns a custom djang.db.models.sql.query.Query subclass that is
    appropriate for Firebird.    
    """
    global _classes
    try:
        return _classes[QueryClass]
    except KeyError:
        pass
    
    class FirebirdQuery(QueryClass):                           
        def as_sql(self, with_limits=True, with_col_aliases=False):
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
    _classes[QueryClass] = FirebirdQuery
    return FirebirdQuery
