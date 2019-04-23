# -*- coding: utf-8 -*-

import os
import psycopg2
import psycopg2.extras

from enum import Enum

from pirogue.utils import table_parts, list2str, select_columns, insert_command, update_command
from pirogue.information_schema import TableHasNoPrimaryKey, columns, reference_columns, primary_key, default_value


def update_columns(columns: list, sep:str=', ') -> str:
    return sep.join(["{c} = NEW.{c}".format(c=col) for col in columns])


class Join:
    """
    Creates a simple join view with associated triggers to edit.
    """

    def __init__(self, parent_table: str, child_table: str,
                 pg_service: str=None,
                 view_schema: str=None,
                 view_name: str=None):
        """
        Produces the SQL code of the join table and triggers
        :param pg_service:
        :param parent_table:
        :param child_table:
        :param view_schema: the schema where the view will written to
        :param view_name: the name of the created view, defaults to vw_{parent_table}_{join_table}
        """

        if pg_service is None:
            pg_service = os.getenv('PGSERVICE')
        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cursor = self.conn.cursor()

        (self.parent_schema, self.parent_table) = table_parts(parent_table)
        (self.child_schema, self.child_table) = table_parts(child_table)

        if view_schema is None:
            if self.parent_schema != self.child_schema:
                raise ValueError('Destination schema cannot be guessed if different on sources tables.')
            else:
                self.view_schema = self.parent_schema
        else:
            self.view_schema = view_schema

        self.view_name = view_name or "vw_{pt}_{ct}".format(pt=self.parent_table, ct=self.child_table)

        (self.ref_parent_key, parent_referenced_key) = reference_columns(self.cursor,
                                                                         self.child_schema, self.child_table,
                                                                         self.parent_schema, self.parent_table)
        try:
            self.child_pkey = primary_key(self.cursor, self.child_schema, self.child_table)
        except TableHasNoPrimaryKey:
            self.child_pkey = self.ref_parent_key
        self.parent_pkey = primary_key(self.cursor, self.parent_schema, self.parent_table)

        assert self.parent_pkey == parent_referenced_key

    def create(self) -> bool:
        """
        :return:
        """

        for sql in [self.__view(),
                    self.__insert_trigger(),
                    self.__update_trigger(),
                    self.__delete_trigger()
                    ]:
            try:
                self.cursor.execute(sql)
            except psycopg2.Error as e:
                print("*** Failing:\n{}\n***".format(sql))
                raise e
        self.conn.commit()
        self.conn.close()
        return True

    def __view(self) -> str:
        """
        Create the SQL code for the view
        :return: the SQL code
        """
        sql = """
CREATE OR REPLACE VIEW {vs}.{vn} AS SELECT
  {parent_cols},
  {child_cols}
  FROM {cs}.{ct}
  LEFT JOIN {ps}.{pt} ON {pt}.{prk} = {ct}.{rpk};
""".format(vs=self.view_schema,
           vn=self.view_name,
           parent_cols=select_columns(self.cursor, self.parent_schema, self.parent_table, table_alias=self.parent_table),
           child_cols=select_columns(self.cursor, self.child_schema, self.child_table, table_alias=self.child_table, skip_columns=[self.ref_parent_key], ),
           cs=self.child_schema,
           ct=self.child_table,
           ps=self.parent_schema,
           pt=self.parent_table,
           rpk=self.ref_parent_key,
           prk=self.parent_pkey)

        return sql

    def __insert_trigger(self) -> str:
        """

        :return:
        """
        sql = """
-- INSERT TRIGGER
CREATE OR REPLACE FUNCTION {vs}.ft_{vn}_insert() RETURNS trigger AS
$BODY$
BEGIN
{insert_parent}

{insert_child}
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_{vn}_on_insert ON {vs}.{vn};

CREATE TRIGGER tr_{vn}_on_insert
  INSTEAD OF INSERT ON {vs}.{vn}
  FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_insert();
""".format(vs=self.view_schema,
           vn=self.view_name,
           insert_parent=insert_command(self.cursor, self.parent_schema, self.parent_table,
                                        remove_pkey=False,
                                        coalesce_pkey_default=True,
                                        returning='{prk} INTO NEW.{prk}'.format(prk=self.parent_pkey)),
           insert_child=insert_command(self.cursor, self.child_schema, self.child_table,
                                       remove_pkey=False,
                                       coalesce_pkey_default=True,
                                       pkey=self.child_pkey,
                                       insert_values={self.ref_parent_key: 'NEW.{c}'.format(c=self.parent_pkey)})
           )
        return sql

    def __update_trigger(self):
        sql = """
-- UPDATE TRIGGER
CREATE OR REPLACE FUNCTION {vs}.ft_{vn}_update() RETURNS trigger AS
$BODY$
BEGIN
{update_master}

{update_child}
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_{vn}_on_update ON {vs}.{vn};

CREATE TRIGGER tr_{vn}_on_update
  INSTEAD OF UPDATE ON {vs}.{vn}
  FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_update();
""".format(vs=self.view_schema,
           vn=self.view_name,
           update_master=update_command(self.cursor, self.parent_schema, self.parent_table),
           update_child=update_command(self.cursor, self.child_schema, self.child_table, pkey=self.child_pkey, remove_pkey=False, update_values={self.ref_parent_key: 'OLD.{c}'.format(c=self.parent_pkey)})
           )
        return sql

    def __delete_trigger(self):
        sql = """
CREATE OR REPLACE FUNCTION {vs}.ft_{vn}_delete() RETURNS trigger AS
$BODY$
BEGIN
  DELETE FROM {cs}.{ct} WHERE {rpk} = OLD.{ppk};
  DELETE FROM {ps}.{pt} WHERE {ppk} = OLD.{ppk};
RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_{vn}_on_delete ON {vs}.{vn};

CREATE TRIGGER tr_{vn}_on_delete
  INSTEAD OF DELETE ON {vs}.{vn}
  FOR EACH ROW EXECUTE PROCEDURE {vs}.ft_{vn}_delete();
""".format(vs=self.view_schema,
           vn=self.view_name,
           cs=self.child_schema,
           ct=self.child_table,
           rpk=self.ref_parent_key,
           ppk=self.parent_pkey,
           ps=self.parent_schema,
           pt=self.parent_table)
        return sql
