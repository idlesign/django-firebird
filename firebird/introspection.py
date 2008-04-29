from django.db import transaction
from django.db.backends.firebird.base import DatabaseOperations

qn = quote_name = DatabaseOperations().quote_name

def get_table_list(cursor):
    "Returns a list of table names in the current database."
    cursor.execute("""
        SELECT rdb$relation_name FROM rdb$relations
        WHERE rdb$system_flag = 0 AND rdb$view_blr IS NULL ORDER BY rdb$relation_name""")
    return [str(row[0].strip()) for row in cursor.fetchall()]

def get_table_description(cursor, table_name):
    "Returns a description of the table, with the DB-API cursor.description interface."
    #cursor.execute("SELECT FIRST 1 * FROM %s" % quote_name(table_name))
    #return cursor.description
    # (name, type_code, display_size, internal_size, precision, scale, null_ok)
    cursor.execute("""
        SELECT DISTINCT R.RDB$FIELD_NAME AS FNAME,
                  F.RDB$FIELD_TYPE AS FTYPE,
                  F.RDB$CHARACTER_LENGTH AS FCHARLENGTH,
                  F.RDB$FIELD_LENGTH AS FLENGTH,
                  F.RDB$FIELD_PRECISION AS FPRECISION,
                  F.RDB$FIELD_SCALE AS FSCALE,
                  R.RDB$NULL_FLAG AS NULL_FLAG,
                  R.RDB$FIELD_POSITION
        FROM RDB$RELATION_FIELDS R
             JOIN RDB$FIELDS F ON R.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME
        WHERE F.RDB$SYSTEM_FLAG=0 and R.RDB$RELATION_NAME STARTING WITH %s
        ORDER BY R.RDB$FIELD_POSITION
    """, (qn(table_name).strip('"'),))
    return [(row[0].rstrip(), row[1], row[2] or 0, row[3], row[4], row[5], row[6] and True or False) for row in cursor.fetchall()]


def get_relations(cursor, table_name):
    """
    Returns a dictionary of {field_index: (field_index_other_table, other_table)}
    representing all relationships to the given table. Indexes are 0-based.
    """
    cursor.execute("""
        SELECT seg.rdb$field_name, seg_ref.rdb$field_name, idx_ref.rdb$relation_name
        FROM rdb$indices idx
        INNER JOIN rdb$index_segments seg
            ON seg.rdb$index_name = idx.rdb$index_name
        INNER JOIN rdb$indices idx_ref
            ON idx_ref.rdb$index_name = idx.rdb$foreign_key
        INNER JOIN rdb$index_segments seg_ref
            ON seg_ref.rdb$index_name = idx_ref.rdb$index_name
        WHERE idx.rdb$relation_name STARTING WITH %s
            AND idx.rdb$foreign_key IS NOT NULL""", [qn(table_name).strip('"')])

    relations = {}
    for row in cursor.fetchall():
        relations[row[0].rstrip()] = (row[1].strip(), row[2].strip())
    return relations


def get_indexes(cursor, table_name):
    """
    Returns a dictionary of fieldname -> infodict for the given table,
    where each infodict is in the format:
        {'primary_key': boolean representing whether it's the primary key,
         'unique': boolean representing whether it's a unique index}
    """

    # This query retrieves each field name and index type on the given table.
    cursor.execute("""
        SELECT seg.RDB$FIELD_NAME, const.RDB$CONSTRAINT_TYPE
        FROM RDB$RELATION_CONSTRAINTS const
        LEFT JOIN RDB$INDEX_SEGMENTS seg
            ON seg.RDB$INDEX_NAME = const.RDB$INDEX_NAME
        WHERE const.RDB$RELATION_NAME STARTING WITH ?
        ORDER BY seg.RDB$FIELD_POSITION)""",
        [qn(table_name).strip('"')])
    indexes = {}
    for row in cursor.fetchall():
        indexes[row[0]] = {'primary_key': row[1].startswith('PRIMARY'),
                           'unique': row[1].startswith('UNIQUE')}    
    return indexes

# Maps type codes to Django Field types.
DATA_TYPES_REVERSE = {
   261: 'LargeTextField',
    14:  'CharField',
    40:  'CharField',
    11:  'FloatField',
    27:  'FloatField',
    10:  'FloatField',
    16:  'IntegerField',
     8:   'IntegerField',
     9:   'IntegerField',
     7:   'SmallIntegerField',
    12:  'DateField',
    13:  'TimeField',
    35:  'DateTimeField',
    37: 'CharField'
}
