from django.db.models.sql import compiler


class SQLCompiler(compiler.SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False):
        """
        Return custom SQL. Use FIRST and SKIP statement instead of
        LIMIT and OFFSET.
        """
        do_offset = with_limits and (self.query.high_mark is not None
                                     or self.query.low_mark)
        if not do_offset:
            sql, params = super(SQLCompiler, self).as_sql(with_limits=False,
                    with_col_aliases=with_col_aliases)
        else:
            sql, params = super(SQLCompiler, self).as_sql(with_limits=False,
                                                    with_col_aliases=True)
            result = ['SELECT']
            if self.query.high_mark is not None:
                result.append('FIRST %d' % (self.query.high_mark - self.query.low_mark))
            if self.query.low_mark:
                if self.query.high_mark is None:
                    val = self.connection.ops.no_limit_value()
                    if val:
                        result.append('FIRST %d' % val)
                result.append('SKIP %d' % self.query.low_mark)
            sql = sql.replace("SELECT", ' '.join(result))

        return sql, params


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass

class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass

class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass

class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass

class SQLDateCompiler(compiler.SQLDateCompiler, SQLCompiler):
    pass
