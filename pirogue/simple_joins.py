# -*- coding: utf-8 -*-

import os
import psycopg2
import psycopg2.extras

from pirogue.utils import table_parts, select_columns
from pirogue.information_schema import reference_columns, primary_key
from pirogue.exceptions import InvalidDefinition, TableHasNoPrimaryKey, NoReferenceFound


class SimpleJoins:
    """
    Creates a view made of simple joins, without any edit triggers.
    """
    def __init__(self,
                 definition: dict,
                 pg_service: str = None):
        """
        Produces the SQL code of the join table and triggers

        Parameters
        ----------
        definition
            the YAML definition of the multiple inheritance
        pg_service
            if not given, it is determined using environment variable PGSERVICE
        """

        # check definition validity
        for key in definition.keys():
            if key not in ('table', 'view_schema','joins', 'pkey', 'view_name'):
                raise InvalidDefinition('key {k} is not a valid'.format(k=key))
        # check joins validity
        for alias, table_def in definition['joins'].items():
            for key in table_def.keys():
                if key not in ('table', 'short_alias',
                               'fkey', 'prefix',
                               'skip_columns', 'remap_columns'):
                    raise InvalidDefinition('in join {a} key "{k}" is not valid'.format(a=alias, k=key))

        if pg_service is None:
            pg_service = os.getenv('PGSERVICE')
        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cursor = self.conn.cursor()

        (self.parent_schema, self.parent_table) = table_parts(definition['table'])
        self.view_schema = definition.get('view_schema', self.parent_schema)
        self.view_name = definition.get('view_name', "vw_{pt}".format(pt=self.parent_table))

        try:
            self.parent_pkey = primary_key(self.cursor, self.parent_schema, self.parent_table)
        except TableHasNoPrimaryKey:
            self.parent_pkey = definition['pkey']

        class Table:
            def __init__(self):
                self.schema_name = None
                self.table_name = None
                self.pkey = None
                self.ref_parent_key = None
                self.parent_referenced_key = None
                self.skip_columns = None
                self.remap_columns = None
                self.prefix = None

        self.child_tables = {}
        for alias, table_def in definition['joins'].items():
            child = Table()
            (child.schema_name, child.table_name) = table_parts(table_def['table'])
            child.pkey = primary_key(self.cursor, child.schema_name, child.table_name)
            try:
                (child.parent_referenced_key, child.ref_parent_key) = reference_columns(self.cursor,
                                                                                        self.parent_schema,
                                                                                        self.parent_table,
                                                                                        child.schema_name,
                                                                                        child.table_name)
                assert child.pkey == child.ref_parent_key
            except NoReferenceFound:
                child.parent_referenced_key = table_def['fkey']

            child.skip_columns = table_def.get('skip_columns', {})
            child.remap_columns = table_def.get('remap_columns', {})
            child.prefix = table_def.get('prefix', None)
            self.child_tables[alias] = child

    def create(self) -> bool:
        """
        Creates the merge view on the specified service
        Returns True in case of success
        """
        sql = self.__view()
        success = True
        try:
            self.cursor.execute(sql)
        except psycopg2.Error as e:
            success = False
            print("*** Failing:\n{}\n***".format(sql))
            raise e
        self.conn.commit()
        self.conn.close()
        return success

    def __view(self) -> str:
        """
        Create the SQL code for the view
        :return: the SQL code
        """
        sql = """
CREATE OR REPLACE VIEW {vs}.{vn} AS SELECT
  {parent_cols}
  {child_cols}
  FROM {ps}.{pt}
  {joins};
""".format(vs=self.view_schema,
           vn=self.view_name,
           parent_cols=select_columns(self.cursor, self.parent_schema, self.parent_table, table_alias=self.parent_table, remove_pkey=False),
           child_cols='\n  '.join([select_columns(self.cursor, child_def.schema_name, child_def.table_name,
                                                   table_alias=alias, remap_columns=child_def.remap_columns,
                                                   prefix=child_def.prefix, separate_first=True)
                                    for alias, child_def in self.child_tables.items()]),
           ps=self.parent_schema,
           pt=self.parent_table,
           joins='\n  '.join(["LEFT JOIN {cs}.{ct} {alias} ON {alias}.{cpk} = {pt}.{rpk}".format(cs=child.schema_name,
                                                                                                 ct=child.table_name,
                                                                                                 alias=alias,
                                                                                                 cpk=child.pkey,
                                                                                                 pt=self.parent_table,
                                                                                                 rpk=child.parent_referenced_key)
                            for alias, child in self.child_tables.items()])
           )

        return sql