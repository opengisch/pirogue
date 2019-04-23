# -*- coding: utf-8 -*-

import os
import psycopg2
import psycopg2.extras

from enum import Enum

from pirogue.utils import table_parts, list2str
from pirogue.information_schema import TableHasNoPrimaryKey, columns, reference_columns, primary_key, default_value


def update_columns(columns: list, sep:str=', ') -> str:
    return sep.join(["{c} = NEW.{c}".format(c=col) for col in columns])


class JoinType(Enum):
    INNER = 'INNER'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    FULL = 'FULL'


class Join:
    """
    Creates a simple join view with associated triggers to edit.
    """

    def __init__(self, master_table: str, joined_table: str,
                 pg_service: str=None,
                 view_schema: str=None,
                 view_name: str=None,
                 join_type: JoinType=JoinType.LEFT):
        """
        Produces the SQL code of the join table and triggers
        :param pg_service:
        :param master_table:
        :param joined_table:
        :param view_schema: the schema where the view will written to
        :param view_name: the name of the created view, defaults to vw_{master_table}_{join_table}
        :param join_type: the type of join
        """

        if pg_service is None:
            pg_service = os.getenv('PGSERVICE')
        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cursor = self.conn.cursor()

        (self.master_schema, self.master_table) = table_parts(master_table)
        (self.joined_schema, self.joined_table) = table_parts(joined_table)

        self.joined_type = join_type

        if view_schema is None:
            if self.master_schema != self.joined_schema:
                raise ValueError('Destination schema cannot be guessed if different on sources tables.')
            else:
                self.view_schema = self.master_schema
        else:
            self.view_schema = view_schema

        self.view_name = view_name or "vw_{ta}_{tb}".format(ta=self.master_table, tb=self.joined_table)

        self.master_cols = columns(self.cursor, self.master_schema, self.master_table)
        self.joined_cols = columns(self.cursor, self.joined_schema, self.joined_table)
        (self.ref_master_key, self.ref_joined_key) = reference_columns(self.cursor, self.master_schema, self.master_table, self.joined_schema, self.joined_table)
        try:
            self.master_pkey = primary_key(self.cursor, self.master_schema, self.master_table)
        except TableHasNoPrimaryKey:
            self.master_pkey = self.ref_master_key
        self.joined_pkey = primary_key(self.cursor, self.joined_schema, self.joined_table)
        self.joined_cols_wo_pkey = columns(self.cursor, self.joined_schema, self.joined_table, remove_pkey=True)
        self.master_cols_wo_pkey = list(self.master_cols)  # make a copy, otherwise keeps reference
        self.master_cols_wo_pkey.remove(self.master_pkey)

    def create(self) -> bool:
        """

        :return:
        """

        for sql in [self.__view(),
                    self.__insert_trigger(),
                    self.__update_trigger(),
                    self.__delete_trigger()]:
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
CREATE OR REPLACE VIEW {ds}.{dt} AS SELECT
  {master_cols}{joined_cols}
  FROM {sm}.{tm} AS m
  {jt} JOIN {sj}.{tj} AS j ON j.{rjk} = m.{rmk};
""".format(ds=self.view_schema,
           dt=self.view_name,
           jt=self.joined_type.value,
           sm=self.master_schema,
           tm=self.master_table,
           rmk=self.ref_master_key,
           master_cols=list2str(self.master_cols, prepend='m.'),
           sj=self.joined_schema,
           tj=self.joined_table,
           rjk=self.ref_joined_key,
           joined_cols=list2str(self.joined_cols_wo_pkey, prepend='j.', prepend_to_list=', '))
        return sql

    def __insert_trigger(self) -> str:
        """

        :return:
        """
        sql = """
-- INSERT TRIGGER
CREATE OR REPLACE FUNCTION {ds}.ft_{dt}_insert() RETURNS trigger AS
$BODY$
BEGIN
INSERT INTO {sj}.{tj} ( {joined_cols} )
  VALUES (
    COALESCE( NEW.{rmk}, {jkp_def} ), {joined_new_cols} )
  RETURNING {jpk} INTO NEW.{rmk};
INSERT INTO {sm}.{tm} ( {master_cols} )
  VALUES (
    COALESCE( NEW.{mpk}, {mkp_def} ),  {master_new_cols} );
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER tr_{dt}_on_insert
  INSTEAD OF INSERT ON {ds}.{dt}
  FOR EACH ROW EXECUTE PROCEDURE {ds}.ft_{dt}_insert();
""".format(ds=self.view_schema,
           dt=self.view_name,
           sm=self.master_schema,
           tm=self.master_table,
           rmk=self.ref_master_key,
           mpk=self.master_pkey,
           master_cols=list2str(self.master_cols, prepend='\n    '),
           mkp_def=default_value(self.cursor, self.master_schema, self.master_table, self.master_pkey),
           master_new_cols=list2str(self.master_cols_wo_pkey, prepend='\n    NEW.', append=''),
           sj=self.joined_schema,
           tj=self.joined_table,
           joined_cols=list2str(self.joined_cols, prepend='\n    '),
           jpk=self.joined_pkey,
           jkp_def=default_value(self.cursor, self.joined_schema, self.joined_table, self.joined_pkey),
           joined_new_cols=list2str(self.joined_cols_wo_pkey, prepend='\n    NEW.', append=''))
        return sql

    def __update_trigger(self):
        sql = """
-- UPDATE TRIGGER
CREATE OR REPLACE FUNCTION {ds}.ft_{dt}_update() RETURNS trigger AS
$BODY$
BEGIN
  UPDATE {sa}.{ta}
    SET {master_up_cols}
    WHERE {apk} = OLD.{apk};
  UPDATE {sb}.{tb}
    SET {joined_up_cols}
    WHERE {bpk} = OLD.{rak};
RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER tr_{dt}_on_update
  INSTEAD OF UPDATE ON {ds}.{dt}
  FOR EACH ROW EXECUTE PROCEDURE {ds}.ft_{dt}_update();
""".format(ds=self.view_schema,
           dt=self.view_name,
           sa=self.master_schema,
           ta=self.master_table,
           apk=self.master_pkey,
           master_up_cols=update_columns(self.master_cols_wo_pkey, sep='\n    , '),
           sb=self.joined_schema,
           tb=self.joined_table,
           bpk=self.joined_pkey,
           rak=self.ref_master_key,
           joined_up_cols=update_columns(self.joined_cols_wo_pkey, sep='\n    , '))
        return sql

    def __delete_trigger(self):
        sql = """
CREATE OR REPLACE FUNCTION {ds}.ft_{dt}_delete() RETURNS trigger AS
$BODY$
BEGIN
  DELETE FROM {sa}.{ta} WHERE {apk} = OLD.{apk};
  DELETE FROM {sb}.{tb} WHERE {bpk} = OLD.{rak};
RETURN NULL;
END;
$BODY$
LANGUAGE plpgsql;

CREATE TRIGGER tr_{dt}_on_delete
  INSTEAD OF DELETE ON {ds}.{dt}
  FOR EACH ROW EXECUTE PROCEDURE {ds}.ft_{dt}_delete();
""".format(ds=self.view_schema,
           dt=self.view_name,
           sa=self.master_schema,
           ta=self.master_table,
           apk=self.master_pkey,
           sb=self.joined_schema,
           tb=self.joined_table,
           bpk=self.joined_pkey,
           rak=self.ref_master_key)
        return sql
