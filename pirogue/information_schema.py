# -*- coding: utf-8 -*-

from psycopg2.extensions import cursor


def primary_key(pg_cur: cursor, schema: str, table: str) -> str:
    """
    Returns the primary of a table
    :param pg_cur: psycopg cursor
    :param schema: the schema
    :param table: the table
    :return:
    """
    sql = "SELECT c.column_name"\
          " FROM information_schema.key_column_usage AS c "\
          " LEFT JOIN information_schema.table_constraints AS t"\
          " ON t.constraint_name = c.constraint_name"\
          " WHERE t.table_name = '{t}'"\
          " AND t.table_schema = '{s}'"\
          " AND t.constraint_type = 'PRIMARY KEY'".format(s=schema, t=table)
    pg_cur.execute(sql)
    pkey = pg_cur.fetchone()[0]
    return pkey


def columns(pg_cur: cursor, schema: str, table: str, remove_pkey: bool=False) -> list:
    """
    Returns the columns of a table
    :param pg_cur: psycopg cursor
    :param schema: the schema
    :param table: the table
    :param remove_pkey: if True, the primary key is dropped
    :return: the list of columns
    """
    pg_cur.execute("SELECT attname"
                   " FROM pg_attribute "
                   " WHERE attrelid = '{s}.{t}'::regclass"
                   " AND attisdropped IS NOT TRUE "
                   " AND attnum > 0 "
                   " ORDER BY attnum ASC"\
                   .format(s=schema, t=table))
    pg_fields = pg_cur.fetchall()
    pg_fields = [field[0] for field in pg_fields]
    if remove_pkey:
        pkey = primary_key(pg_cur, schema, table)
        pg_fields.remove(pkey)
    return pg_fields


