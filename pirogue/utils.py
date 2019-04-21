# -*- coding: utf-8 -*-

from psycopg2.extensions import cursor
from enum import Enum
from .information_schema import columns


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
    :param remap_columns: dictionary to remap columns
    :param insert_values: dictionary of expression to be used at insert
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

    for col in insert_values.keys():
        if col not in cols:
            raise InvalidColumn('Invalid column in insert_values paramater: "{tab}" has no column "{col}"'
                                .format(tab=table_name, col=col))
    return """{indent}INSERT INTO {s}.{t} (
{cols} ) 
{indent}VALUES ( 
{new_cols} );
""".format(indent=indent*' ',
           s=table_schema,
           t=table_name,
           cols=',\n'.join(['{indent}    {col}'.format(indent=indent*' ', col=col) for col in cols]),
           new_cols=',\n'.join(['{indent}    {value}'
                               .format(indent=indent*' ',
                                       value=insert_values.get(col,
                                                               'NEW.{cal}'.format(cal=column_alias(col,
                                                                                                   remap_columns=remap_columns,
                                                                                                   prefix=prefix,
                                                                                                   field_if_no_alias=True))))
                                for col in cols]))


def update_columns(columns: list, sep:str=', ') -> str:
    return sep.join(["{c} = NEW.{c}".format(c=col) for col in columns])


def __column_priority(column: str, columns_on_top: list=[], columns_at_end: list=[]) -> int:
    if column in columns_on_top:
        return 0
    elif column in columns_at_end:
        return 2
    else:
        return 1