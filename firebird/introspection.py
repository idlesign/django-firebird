from django.db.backends import BaseDatabaseIntrospection

class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type codes to Django Field types.
    data_types_reverse = {
        7: 'SmallIntegerField',
        8: 'IntegerField',
        10: 'FloatField',
        12: 'DateField',
        13: 'TimeField',
        14: 'TextField',
        16: 'IntegerField',
        27: 'FloatField',
        35: 'DateTimeField',
        37: 'CharField',
        40: 'TextField',
        261: 'TextField',
    }

    def get_table_list(self, cursor):
        "Returns a list of table names in the current database."
        cursor.execute("""select rdb$relation_name from rdb$relations
            where rdb$system_flag=0 and rdb$view_source is null
            order by rdb$relation_name""")
        return [r[0].strip().lower() for r in cursor.fetchall()]

    def get_table_description(self, cursor, table_name):
        "Returns a description of the table, with the DB-API cursor.description interface."
        #tbl_name = self.connection.ops.quote_name(table_name)
        tbl_name = "'%s'" % table_name.upper()
        cursor.execute("""select A.rdb$field_name,
            B.rdb$field_type, B.rdb$field_length, B.rdb$field_precision,
            B.rdb$field_scale, A.rdb$null_flag
            from rdb$relation_fields A, rdb$fields B
            where A.rdb$field_source = B.rdb$field_name
                and  upper(A.rdb$relation_name) = %s
            order by A.rdb$field_position
            """ % (tbl_name,))
        return [(r[0].strip().lower(), r[1], r[2], r[2] or 0, r[3], r[4], r[5] and True or False) for r in cursor.fetchall()]

    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        #tbl_name = self.connection.ops.quote_name(table_name)
        tbl_name = "'%s'" % table_name.upper()
        cursor.execute("""
            SELECT seg.rdb$field_name, seg_ref.rdb$field_name, idx_ref.rdb$relation_name
            FROM rdb$indices idx
            INNER JOIN rdb$index_segments seg
                ON seg.rdb$index_name = idx.rdb$index_name
            INNER JOIN rdb$indices idx_ref
                ON idx_ref.rdb$index_name = idx.rdb$foreign_key
            INNER JOIN rdb$index_segments seg_ref
                ON seg_ref.rdb$index_name = idx_ref.rdb$index_name
            WHERE idx.rdb$relation_name = %s
                AND idx.rdb$foreign_key IS NOT NULL""", (tbl_name,))

        relations = {}
        for r in cursor.fetchall():
            relations[r[0].strip()] = (r[1].strip(), r[2].strip())
        return relations

    def get_indexes(self, cursor, table_name):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index}
        """

        # This query retrieves each field name and index type on the given table.
        #tbl_name = self.connection.ops.quote_name(table_name)
        tbl_name = "'%s'" % table_name.upper()
        cursor.execute("""
            SELECT seg.rdb$field_name, const.rdb$constraint_type
            FROM rdb$relation_constraints const
            LEFT JOIN rdb$index_segments seg
                ON seg.rdb$index_name = const.rdb$index_name
            WHERE const.rdb$relation_name = %s
                AND (const.rdb$constraint_type = 'PRIMARY KEY'
                    OR const.rdb$constraint_type = 'UNIQUE')""", (tbl_name,))
        indexes = {}
        for r in cursor.fetchall():
            indexes[r[0].strip()] = {
                'primary_key': ('PRIMARY KEY' == r[1].strip()),
                'unique': ('UNIQUE' == r[1].strip())}
        return indexes
