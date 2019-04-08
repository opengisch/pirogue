# -*- coding: utf-8 -*-

from psycopg2.extensions import cursor


class TableHasNoPrimaryKey(Exception):
    pass


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
    try:
        pkey = pg_cur.fetchone()[0]
    except Exception:
        raise TableHasNoPrimaryKey(sql)
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


def reference_columns(pg_cur: cursor,
                      table_schema: str, table_name: str,
                      foreign_table_schema: str, foreign_table_name: str) -> (str, str):
    """
    Returns the columns use in a reference constraint
    :param pg_cur:
    :param table_schema:
    :param table_name:
    :param foreign_table_schema:
    :param foreign_table_name:
    :return:
    """
    # see https://stackoverflow.com/a/1152321/1548052
    sql = "SELECT kcu.column_name, ccu.column_name AS foreign_column_name " \
          "FROM information_schema.table_constraints AS tc " \
          "JOIN information_schema.key_column_usage AS kcu " \
          "ON tc.constraint_name = kcu.constraint_name " \
          "AND tc.table_schema = kcu.table_schema " \
          "JOIN information_schema.constraint_column_usage AS ccu " \
          "ON ccu.constraint_name = tc.constraint_name " \
          "AND ccu.table_schema = tc.table_schema " \
          "WHERE tc.constraint_type = 'FOREIGN KEY' " \
          "AND tc.table_name='{tn}' " \
          "AND tc.table_schema='{ts}' " \
          "AND ccu.table_name = '{ftn}' " \
          "AND ccu.table_schema = '{fts}';".format(tn=table_name,
                                                   ts=table_schema,
                                                   ftn=foreign_table_name,
                                                   fts=foreign_table_schema)
    pg_cur.execute(sql)
    cols = pg_cur.fetchone()
    assert len(cols) == 2
    return cols


def default_value(pg_cur: cursor, table_schema: str, table_name: str, column: str) -> str:
    """
    Returns the default value of the column
    :param pg_cur:
    :param table_schema:
    :param table_name:
    :param column:
    :return:
    """
    # see https://stackoverflow.com/a/8148177/1548052

    sql = "SELECT d.adsrc AS default_value\n" \
          "FROM pg_catalog.pg_attribute a\n" \
          "LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid,  d.adnum)\n" \
          "WHERE  NOT a.attisdropped   -- no dropped (dead) columns\n" \
          "AND    a.attnum > 0         -- no system columns\n" \
          "AND    a.attrelid = '{ts}.{tn}'::regclass\n" \
          "AND    a.attname = '{col}';" \
        .format(ts=table_schema,
                tn=table_name,
                col=column)
    pg_cur.execute(sql)
    return pg_cur.fetchone()[0] or 'NULL'
