# -*- coding: utf-8 -*-
from psycopg2.extensions import cursor

from pirogue.exceptions import InvalidColumn
from pirogue.information_schema import columns, primary_key, default_value


def table_parts(name: str) -> (str, str):
    """
    Returns a tuple with schema and table names

    Parameters
    ----------
    name
        the schema specified name of the table or view.
    """
    if name and '.' in name:
        return name.split('.', 1)
    else:
        return 'public', name


def select_columns(pg_cur: cursor,
                   table_schema: str,
                   table_name: str,
                   table_type: str = 'table',
                   table_alias: str = None,
                   remove_pkey: bool = False,
                   skip_columns: list = [],
                   safe_skip_columns: list = [],
                   columns_list: list = None,
                   comment_skipped: bool = True,
                   remap_columns: dict = {},
                   columns_on_top: list = [],
                   columns_at_end: list = [],
                   prefix: str = None,
                   indent: int = 2) -> str:
    """
    Returns the list of columns to be used in a SELECT command

    Parameters
    ----------
    pg_cur
        the psycopg cursor
    table_schema
        the schema
    table_name
        the name of the table
    table_type
        the type of table, i.e. view or table
    table_alias
        if not specified, table is used
    remove_pkey
        if True, the primary is removed from the list
    skip_columns
        list of columns to be skipped, raise an exception if the column does not exist
    safe_skip_columns
        list of columns to be skipped, do not raise exception if column does not exist
    columns_list
        if given use as list of columns
    comment_skipped
        if True, skipped columns are written but commented, otherwise they are not written
        If remove_pkey is True, the primary key will not be printed
    remap_columns
        dictionary to remap columns
    columns_on_top
        bring the columns to the front of the list
    columns_at_end
        bring the columns to the end of the list
    prefix
        add a prefix to the columns (do not applied to remapped columns)
    indent
        add an indent in front
    """
    cols = sorted(columns_list or columns(pg_cur,
                                          table_schema=table_schema,
                                          table_name=table_name,
                                          table_type=table_type,
                                          remove_pkey=remove_pkey),
                  key=lambda col: __column_priority(col))
    cols=[col for col in cols if col not in safe_skip_columns]

    # check arguments
    for param, dict_or_list in {'skip_columns': skip_columns,
                                'remap_columns': remap_columns,
                                'columns_on_top': columns_on_top,
                                'columns_at_end': columns_at_end}.items():
        for col in dict_or_list:
            if col not in cols:
                raise InvalidColumn('Invalid column in {param} paramater: "{tab}" has no column "{col}"'
                                    .format(param=param, tab=table_name, col=col))

    first_column_printed = [False]

    def print_comma(first_column_printed, print: bool) -> str:
        if first_column_printed[0]:
            # we can print in any case
            return ', '
        elif print:
            # we start printing commas (i.e. not commenting anymore)
            if not first_column_printed[0]:
                # this is the first column to be printed => no comma
                first_column_printed[0] = True
                return ''
            else:
                return ', '
        else:
            return ''

    return '\n{indent}'\
        .format(indent=indent*' ')\
        .join(['{skip}{comma}{table_alias}.{column}{col_alias}'
              .format(comma=print_comma(first_column_printed, col not in skip_columns),
                      skip='-- ' if col in skip_columns else '',
                      table_alias=table_alias or table_name,
                      column=col,
                      col_alias=__column_alias(col, remap_columns=remap_columns, prefix=prefix, prepend_as=True))
               for col in cols if (comment_skipped or col not in skip_columns)])


def insert_command(pg_cur: cursor,
                   table_schema: str,
                   table_name: str,
                   table_type: str = 'table',
                   table_alias: str = None,
                   remove_pkey: bool = True,
                   pkey: str = None,
                   coalesce_pkey_default: bool = False,
                   skip_columns: list = [],
                   comment_skipped: bool = True,
                   remap_columns: dict = {},
                   insert_values: dict = {},
                   columns_on_top: list=[],
                   columns_at_end: list=[],
                   prefix: str = None,
                   returning: str = None,
                   indent: int = 2) -> str:
    """

    :param pg_cur: the psycopg cursor
    :param table_schema: the schema
    :param table_name: the name of the table
    :param table_type: the type of table, i.e. view or table
    :param table_alias: the alias of the table
    :param remove_pkey: if True, the primary is removed from the list
    :param pkey: can be manually specified.
    :param coalesce_pkey_default: if True, the following expression is used to insert the primary key: COALESCE( NEW.{pkey}, {default_value} )
    :param skip_columns: list of columns to be skipped
    :param comment_skipped: if True, skipped columns are written but commented, otherwise they are not written
    :param remap_columns: dictionary to remap columns
    :param insert_values: dictionary of expression to be used at insert
    :param columns_on_top: bring the columns to the front of the list
    :param columns_at_end: bring the columns to the end of the list
    :param prefix: add a prefix to the columns (do not applied to remapped columns)
    :param returning: returning command
    :param indent: add an indent in front
    :return:
    """
    remove_pkey = remove_pkey and pkey is None

    # get columns
    cols = sorted(columns(pg_cur,
                          table_schema=table_schema,
                          table_name=table_name,
                          table_type=table_type,
                          remove_pkey=remove_pkey),
                  key=lambda col: __column_priority(col))

    if pkey and remove_pkey:
        cols.remove(pkey)

    if not pkey and coalesce_pkey_default:
        pkey = primary_key(pg_cur, table_schema, table_name)

    # check arguments
    for param, dict_or_list in {'skip_columns': skip_columns,
                                'remap_columns': remap_columns,
                                'insert_values': insert_values,
                                'columns_on_top': columns_on_top,
                                'columns_at_end': columns_at_end}.items():
        for col in dict_or_list:
            if col not in cols:
                raise InvalidColumn('Invalid column in {param} paramater: "{tab}" has no column "{col}"'
                                    .format(param=param, tab=table_name, col=col))

    def value(col):
        if col in insert_values:
            return '{val} -- {ori_col}'.format(val=insert_values[col], ori_col=col)
        cal = __column_alias(col, remap_columns=remap_columns, prefix=prefix, field_if_no_alias=True)
        if coalesce_pkey_default and col == pkey:
            return 'COALESCE( NEW.{cal}, {pk_def} )'.format(cal=cal,
                                                            pk_def=default_value(pg_cur, table_schema, table_name, pkey))
        else:
            return 'NEW.{cal}'.format(cal=cal)

    next_comma_printed_1 = [False]
    next_comma_printed_2 = [False]
    return """INSERT INTO {s}.{t} (
{indent}      {cols} 
{indent}  ) VALUES ( 
{indent}      {new_cols}
{indent}  ){returning};
""".format(indent=indent*' ',
           s=table_schema,
           t=table_name,
           cols='\n{indent}    '
                .format(indent=indent*' ')
                .join(['{skip}{comma}{col}'
                      .format(indent=indent*' ',
                              skip='-- ' if col in skip_columns else '',
                              comma=', ' if __print_comma(next_comma_printed_1, col in skip_columns) else '',
                              col=col)
                       for col in cols if (comment_skipped or col not in skip_columns)]),
           new_cols='\n{indent}    '
                    .format(indent=indent*' ')
                    .join(['{skip}{comma}{value}'
                          .format(skip='-- ' if col in skip_columns else '',
                                  comma=', ' if __print_comma(next_comma_printed_2, col in skip_columns) else '',
                                  value=value(col))
                           for col in cols if (comment_skipped or col not in skip_columns)]),
           returning=' RETURNING {returning}'.format(indent=4*' ', returning=returning) if returning else '')


def update_command(pg_cur: cursor,
                   table_schema: str,
                   table_name: str,
                   table_alias: str = None,
                   table_type: str = 'table',
                   remove_pkey: bool = True,
                   pkey: str = None,
                   skip_columns: list=[],
                   comment_skipped: bool = True,
                   remap_columns: dict = {},
                   update_values: dict = {},
                   columns_on_top: list=[],
                   columns_at_end: list=[],
                   prefix: str= None,
                   where_clause: str = None,
                   indent: int=2) -> str:
    """
    Creates an UPDATE command
    :param pg_cur: the psycopg cursor
    :param table_schema: the schema
    :param table_name: the name of the table
    :param table_type: the type of table, i.e. view or table
    :param remove_pkey: if True, the primary key will also be updated
    :param pkey: can be manually specified.
    :param table_alias: if not specified, table is used
    :param skip_columns: list of columns to be skipped
    :param comment_skipped: if True, skipped columns are written but commented, otherwise they are not written
    :param remap_columns: dictionary to remap columns
    :param update_values: dictionary of expression to be used at insert
    :param columns_on_top: bring the columns to the front of the list
    :param columns_at_end: bring the columns to the end of the list
    :param prefix: add a prefix to the columns (do not applied to remapped columns)
    :param where_clause: can be manually specified
    :param indent: add an indent in front
    :return: the SQL command
    """

    remove_pkey = remove_pkey and pkey is None and where_clause is None
    # get columns
    cols = sorted(columns(pg_cur,
                          table_schema=table_schema,
                          table_name=table_name,
                          table_type=table_type,
                          remove_pkey=remove_pkey),
                  key=lambda _col: __column_priority(_col))

    if pkey and remove_pkey:
        cols.remove(pkey)

    if not pkey and not where_clause:
        pkey = primary_key(pg_cur, table_schema, table_name)

    # check arguments
    for param, dict_or_list in {'skip_columns': skip_columns,
                                'remap_columns': remap_columns,
                                'update_values': update_values,
                                'columns_on_top': columns_on_top,
                                'columns_at_end': columns_at_end}.items():
        for col in dict_or_list:
            if col not in cols and col != pkey:
                raise InvalidColumn('Invalid column in {param} paramater: "{tab}" has no column "{col}"'
                                    .format(param=param, tab=table_name, col=col))

    next_comma_printed = [False]

    return """UPDATE {s}.{t}{a} SET
{indent}    {cols}
{indent}  WHERE {where_clause};"""\
        .format(indent=indent*' ',
                s=table_schema,
                t=table_name,
                a=' {alias}'.format(alias=table_alias) if table_alias else '',
                cols='\n{indent}    '
                     .format(indent=indent*' ')
                     .join(['{skip}{comma}{col} = {new_col}'
                                .format(indent=indent*' ',
                                        skip='-- ' if col in skip_columns else '',
                                        comma=', ' if __print_comma(next_comma_printed, col in skip_columns) else '',
                                        col=col,
                                        new_col=update_values.get(col,
                                                                  'NEW.{cal}'.format(cal=__column_alias(col,
                                                                                                        remap_columns=remap_columns,
                                                                                                        prefix=prefix,
                                                                                                        field_if_no_alias=True))))
                                for col in cols if (comment_skipped or col not in skip_columns)]),
                where_clause=where_clause or '{pkey} = {pkal}'.format(pkey=pkey,
                                                                      pkal=update_values.get(pkey,
                                                                                             'OLD.{cal}'.format(cal=__column_alias(pkey,
                                                                                                                                   remap_columns=remap_columns,
                                                                                                                                   prefix=prefix,
                                                                                                                                   field_if_no_alias=True)))))


def __column_alias(column: str,
                   remap_columns: dict = {},
                   prefix: str= None,
                   field_if_no_alias: bool = False,
                   prepend_as: bool = False) -> list:
    """

    :param table_alias:
    :param column:
    :param field_if_no_alias: if True, return the field if the alias doesn't exist. If False return an empty string
    :param prepend_as: prepend " AS " to the alias
    :return: empty string if there is no alias and (i.e = field name)
    """
    col_alias = ''
    if column in remap_columns:
        col_alias = remap_columns[column]
    elif prefix:
        col_alias = prefix + column
    elif field_if_no_alias:
        col_alias = column
    if prepend_as and col_alias:
        col_alias = ' AS {al}'.format(al=col_alias)
    return col_alias


def __column_priority(column: str, columns_on_top: list=[], columns_at_end: list=[]) -> int:
    if column in columns_on_top:
        return 0
    elif column in columns_at_end:
        return 2
    else:
        return 1


def __print_comma(next_comma_printed: list, is_skipped: bool) -> bool:
    """
    Determines if a comma should be printed
    :param next_comma_printed: a list with a single boolean (works by reference)
    :param is_skipped:
    :return:
    """
    if is_skipped:
        return next_comma_printed[0]
    elif not next_comma_printed[0]:
        next_comma_printed[0] = True
        return False
    else:
        return True