# -*- coding: utf-8 -*-

from psycopg2.extensions import cursor
from enum import Enum
from .information_schema import columns, primary_key


class InvalidColumn(Exception):
    pass


def table_parts(name: str) -> (str, str):
    """
    Returns a tuple with schema and table names
    :param name:
    :return:
    """
    if name and '.' in name:
        return name.split('.', 1)
    else:
        return 'public', name


def list2str(elements: list, sep: str= ', ', prepend: str='', append: str='', prepend_to_list: str='') -> str:
    """
    Prepend to all strings in the list
    :param elements:
    :param sep: separator
    :param prepend:
    :param append:
    :param prepend_to_list: prepend to the return string, if elements is not None or empty
    :return:
    """
    if elements is None or len(elements) == 0:
        return ''
    return prepend_to_list + sep.join([prepend+x+append for x in elements])


def column_alias(column: str,
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


def select_columns(pg_cur: cursor,
                   table_schema: str,
                   table_name: str,
                   table_alias: str=None,
                   table_type: str = 'table',
                   remove_pkey: bool=True,
                   skip_columns: list=[],
                   remap_columns: dict = {},
                   columns_on_top: list=[],
                   columns_at_end: list=[],
                   prefix: str= None,
                   indent: int=2) -> str:
    """

    :param pg_cur: the psycopg cursor
    :param table_schema: the schema
    :param table_name: the name of the table
    :param table_type: the type of table, i.e. view or table
    :param table_alias: if not specified, table is used
    :param remove_pkey: if True, the primary is removed from the list
    :param skip_columns: list of columns to be skipped
    :param remap_columns: dictionary to remap columns
    :param columns_on_top: bring the columns to the front of the list
    :param columns_at_end: bring the columns to the end of the list
    :param prefix: add a prefix to the columns (do not applied to remapped columns)
    :param indent: add an indent in front
    :return:
    """
    cols = sorted(columns(pg_cur,
                          table_schema=table_schema,
                          table_name=table_name,
                          table_type=table_type,
                          remove_pkey=remove_pkey,
                          skip_columns=skip_columns),
                  key=lambda col: __column_priority(col))

    # check arguments
    for param, dict_or_list in {'remap_columns': remap_columns,
                                'columns_on_top': columns_on_top,
                                'columns_at_end': columns_at_end}.items():
        for col in dict_or_list:
            if col not in cols:
                raise InvalidColumn('Invalid column in {param} paramater: "{tab}" has no column "{col}"'
                                    .format(param=param, tab=table_name, col=col))


    return ',\n'.join(['{indent}{table_alias}.{column}{col_alias}'
                      .format(indent=indent*' ',
                              table_alias=table_alias or table_name,
                              column=col,
                              col_alias=column_alias(col, remap_columns=remap_columns, prefix=prefix, prepend_as=True))
                       for col in cols])


def insert_command(pg_cur: cursor,
                   table_schema: str,
                   table_name: str,
                   table_alias: str=None,
                   table_type: str = 'table',
                   remove_pkey: bool=True,
                   skip_columns: list=[],
                   comment_skipped: bool = True,
                   remap_columns: dict = {},
                   insert_values: dict = {},
                   columns_on_top: list=[],
                   columns_at_end: list=[],
                   prefix: str= None,
                   indent: int=2) -> str:
    """

    :param pg_cur: the psycopg cursor
    :param table_schema: the schema
    :param table_name: the name of the table
    :param table_type: the type of table, i.e. view or table
    :param table_alias: if not specified, table is used
    :param remove_pkey: if True, the primary is removed from the list
    :param skip_columns: list of columns to be skipped
    :param comment_skipped: if True, skipped columns are written but commented, otherwise they are not written
    :param remap_columns: dictionary to remap columns
    :param insert_values: dictionary of expression to be used at insert
    :param columns_on_top: bring the columns to the front of the list
    :param columns_at_end: bring the columns to the end of the list
    :param prefix: add a prefix to the columns (do not applied to remapped columns)
    :param indent: add an indent in front
    :return:
    """
    # get columns
    cols = sorted(columns(pg_cur,
                          table_schema=table_schema,
                          table_name=table_name,
                          table_type=table_type,
                          remove_pkey=remove_pkey),
                  key=lambda col: __column_priority(col))

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

    return """{indent}INSERT INTO {s}.{t} (
{indent}      {cols} 
{indent}) VALUES ( 
{indent}      {new_cols}
{indent});
""".format(indent=indent*' ',
           s=table_schema,
           t=table_name,
           cols='\n{indent}    '
                .format(indent=indent*' ')
                .join(['{skip}{comma}{col}'.format(indent=indent*' ',
                                                   skip='-- ' if col in skip_columns else '',
                                                   comma=', ' if i else '',
                                                   col=col)
                            for i, col in enumerate(cols) if (comment_skipped or col not in skip_columns)]),
           new_cols='\n{indent}    '
                    .format(indent=indent*' ')
                    .join(['{skip}{comma}{value}'
                          .format(skip='-- ' if col in skip_columns else '',
                                  comma=', ' if i else '',
                                  value=insert_values.get(col,
                                                          'NEW.{cal}'.format(cal=column_alias(col,
                                                                                              remap_columns=remap_columns,
                                                                                              prefix=prefix,
                                                                                              field_if_no_alias=True))))
                                for i, col in enumerate(cols) if (comment_skipped or col not in skip_columns)]))


def update_command(pg_cur: cursor,
                   table_schema: str,
                   table_name: str,
                   table_alias: str=None,
                   table_type: str = 'table',
                   update_pkey: bool = False,
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
    :param update_pkey: if True, the primary key will also be updated
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
    # get columns
    cols = sorted(columns(pg_cur,
                          table_schema=table_schema,
                          table_name=table_name,
                          table_type=table_type,
                          remove_pkey=not update_pkey and table_type != 'view'),
                  key=lambda col: __column_priority(col))

    pkey = '' if where_clause else primary_key(pg_cur, table_schema, table_name)

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
    def print_comma(next_comma_printed: bool, is_skipped: bool) -> bool:
        if is_skipped:
            return next_comma_printed[0]
        elif not next_comma_printed[0]:
            next_comma_printed[0] = True
            return False
        else:
            return True

    return """UPDATE {s}.{t} {a} SET
{indent}    {cols}
{indent}  WHERE {where_clause};"""\
        .format(indent=indent*' ',
                s=table_schema,
                t=table_name,
                a=table_alias,
                cols='\n{indent}    '
                     .format(indent=indent*' ')
                     .join(['{skip}{comma}{col} = {new_col}'
                                .format(indent=indent*' ',
                                        skip='-- ' if col in skip_columns else '',
                                        comma=', ' if print_comma(next_comma_printed, col in skip_columns) else '',
                                        col=col,
                                        new_col=update_values.get(col,
                                                                  'NEW.{cal}'.format(cal=column_alias(col,
                                                                                                      remap_columns=remap_columns,
                                                                                                      prefix=prefix,
                                                                                                      field_if_no_alias=True))))
                                for i, col in enumerate(cols) if (comment_skipped or col not in skip_columns)]),
                where_clause=where_clause or '{pkey} = {pkal}'.format(pkey=pkey,
                                                                      pkal=update_values.get(pkey,
                                                                                             'OLD.{cal}'.format(cal=column_alias(pkey,
                                                                                                                                 remap_columns=remap_columns,
                                                                                                                                 prefix=prefix,
                                                                                                                                 field_if_no_alias=True)))))



def update_columns(columns: list, sep:str=', ') -> str:
    return sep.join(["{c} = NEW.{c}".format(c=col) for col in columns])


def __column_priority(column: str, columns_on_top: list=[], columns_at_end: list=[]) -> int:
    if column in columns_on_top:
        return 0
    elif column in columns_at_end:
        return 2
    else:
        return 1