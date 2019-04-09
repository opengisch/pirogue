# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras

from enum import Enum

from pirogue.utils import table_parts, list2str, update_columns
from pirogue.information_schema import TableHasNoPrimaryKey, columns, reference_columns, primary_key, default_value


class JoinType(Enum):
    INNER = 'INNER'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    FULL = 'FULL'


class Join:
    """
    Creates a simple join view with associated triggers to edit.
    """

    def __init__(self, pg_service: str, table_a: str, table_b: str,
                 view_schema: str=None,
                 view_name: str=None,
                 join_type: JoinType=JoinType.LEFT):
        """
        Produces the SQL code of the join table and triggers
        :param pg_service:
        :param table_a:
        :param table_b:
        :param view_schema: the schema where the view will written to
        :param view_name: the name of the created view, defaults to vw_{table_a}_{table_b}
        :param join_type: the type of join
        """

        (self.schema_a, self.table_a) = table_parts(table_a)
        (self.schema_b, self.table_b) = table_parts(table_b)

        self.join_type = join_type

        if view_schema is None:
            if self.schema_a != self.schema_b:
                raise ValueError('Destination schema cannot be guessed if different on sources tables.')
            else:
                self.view_schema = self.schema_a
        else:
            self.view_schema = view_schema

        self.view_name = view_name or "vw_{ta}_{tb}".format(ta=self.table_a, tb=self.table_b)

        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cur = self.conn.cursor()

        self.a_cols = columns(self.cur, self.schema_a, self.table_a)
        self.b_cols = columns(self.cur, self.schema_b, self.table_b)
        (self.ref_a_key, self.ref_b_key) = reference_columns(self.cur, self.schema_a, self.table_a, self.schema_b, self.table_b)
        try:
            self.a_pkey = primary_key(self.cur, self.schema_a, self.table_a)
        except TableHasNoPrimaryKey:
            self.a_pkey = self.ref_a_key
        self.b_pkey = primary_key(self.cur, self.schema_b, self.table_b)
        self.b_cols_wo_pkey = columns(self.cur, self.schema_b, self.table_b, True)
        self.a_cols_wo_pkey = list(self.a_cols)  # make a copy, otherwise keeps reference
        self.a_cols_wo_pkey.remove(self.a_pkey)

    def create(self) -> bool:
        """

        :return:
        """

        for sql in [self.__view(),
                    self.__insert_trigger(),
                    self.__update_trigger(),
                    self.__delete_trigger()]:
            try:
                self.cur.execute(sql)
            except psycopg2.Error as e:
                print("*** Failing:\n{}\n***".format(sql))
                raise e
        self.conn.commit()
        return True

    def __view(self) -> str:
        """
        Create the SQL code for the view
        :return: the SQL code
        """
        sql = "CREATE OR REPLACE VIEW {ds}.{dt} AS SELECT\n  {a_cols}{b_cols}\n" \
              "  FROM {sa}.{ta} AS a\n" \
              "  {jt} JOIN {sb}.{tb} AS b ON b.{rbk} = a.{rak};\n\n"\
            .format(ds=self.view_schema,
                    dt=self.view_name,
                    jt=self.join_type.value,
                    sa=self.schema_a,
                    ta=self.table_a,
                    rak=self.ref_a_key,
                    a_cols=list2str(self.a_cols, prepend='a.'),
                    sb=self.schema_b,
                    tb=self.table_b,
                    rbk=self.ref_b_key,
                    b_cols=list2str(self.b_cols_wo_pkey, prepend='b.', prepend_to_list=', '))
        return sql

    def __insert_trigger(self) -> str:
        """

        :return:
        """
        sql = "-- INSERT TRIGGER\n" \
              "CREATE OR REPLACE FUNCTION {ds}.ft_{dt}_insert() RETURNS trigger AS\n" \
              "$BODY$\n" \
              "BEGIN\n" \
              "INSERT INTO {sb}.{tb}\n" \
              "     ( {b_cols} )\n" \
              "  VALUES (\n" \
              "    COALESCE( NEW.{rak}, {bkp_def} ),\n" \
              "    {b_new_cols}\n" \
              "  )\n" \
              "  RETURNING {bpk} INTO NEW.{rak};\n" \
              "INSERT INTO {sa}.{ta} ( {a_cols} )\n" \
              "  VALUES ( {a_new_cols} );\n" \
              "RETURN NEW;\n" \
              "END;\n" \
              "$BODY$\n" \
              "LANGUAGE plpgsql;\n\n" \
              "CREATE TRIGGER tr_{dt}_on_insert\n" \
              "  INSTEAD OF INSERT ON {ds}.{dt}\n" \
              "  FOR EACH ROW EXECUTE PROCEDURE {ds}.ft_{dt}_insert();\n\n"\
            .format(ds=self.view_schema,
                    dt=self.view_name,
                    sa=self.schema_a,
                    ta=self.table_a,
                    rak=self.ref_a_key,
                    a_cols=list2str(self.a_cols),
                    a_new_cols=list2str(self.a_cols, prepend='NEW.'),
                    sb=self.schema_b,
                    tb=self.table_b,
                    b_cols=list2str(self.b_cols),
                    bpk=self.b_pkey,
                    bkp_def=default_value(self.cur, self.schema_b, self.table_b, self.b_pkey),
                    b_new_cols=list2str(self.b_cols_wo_pkey, prepend='NEW.'))
        return sql

    def __update_trigger(self):
        sql = "-- UPDATE TRIGGER\n" \
              "CREATE OR REPLACE FUNCTION {ds}.ft_{dt}_update() RETURNS trigger AS\n " \
              "$BODY$\n" \
              "BEGIN\n" \
              "  UPDATE {sa}.{ta}\n    SET {a_up_cols}\n    WHERE {apk} = OLD.{apk};\n" \
              "  UPDATE {sb}.{tb}\n    SET {b_up_cols}\n    WHERE {bpk} = OLD.{rak};\n" \
              "RETURN NEW;\n" \
              "END;\n" \
              "$BODY$\n" \
              "LANGUAGE plpgsql;\n\n" \
              "CREATE TRIGGER tr_{dt}_on_update\n" \
              "  INSTEAD OF UPDATE ON {ds}.{dt}\n" \
              "  FOR EACH ROW EXECUTE PROCEDURE {ds}.ft_{dt}_update();\n\n" \
            .format(ds=self.view_schema,
                    dt=self.view_name,
                    sa=self.schema_a,
                    ta=self.table_a,
                    apk=self.a_pkey,
                    a_up_cols=update_columns(self.a_cols_wo_pkey),
                    sb=self.schema_b,
                    tb=self.table_b,
                    bpk=self.b_pkey,
                    rak=self.ref_a_key,
                    b_up_cols=update_columns(self.b_cols_wo_pkey))
        return sql

    def __delete_trigger(self):
        sql = "CREATE OR REPLACE FUNCTION {ds}.ft_{dt}_delete() RETURNS trigger AS\n" \
              "$BODY$\n" \
              "BEGIN\n" \
              "  DELETE FROM {sa}.{ta} WHERE {apk} = OLD.{apk};\n" \
              "  DELETE FROM {sb}.{tb} WHERE {bpk} = OLD.{rak};\n" \
              "RETURN NULL;\n" \
              "END;\n" \
              "$BODY$\n" \
              "LANGUAGE plpgsql;\n\n" \
              "CREATE TRIGGER tr_{dt}_on_delete\n" \
              "  INSTEAD OF DELETE ON {ds}.{dt}\n" \
              "  FOR EACH ROW EXECUTE PROCEDURE {ds}.ft_{dt}_delete();\n\n" \
            .format(ds=self.view_schema,
                    dt=self.view_name,
                    sa=self.schema_a,
                    ta=self.table_a,
                    apk=self.a_pkey,
                    sb=self.schema_b,
                    tb=self.table_b,
                    bpk=self.b_pkey,
                    rak=self.ref_a_key)
        return sql
