# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras

from enum import Enum

from pirogue.utils import table_parts, list2str
from pirogue.information_schema import columns, reference_columns, primary_key, default_value


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
                 output_schema: str=None,
                 join_type: JoinType=JoinType.LEFT):
        """
        Produces the SQL code of the join table and triggers
        :param pg_service:
        :param table_a:
        :param table_b:
        :param output_schema: the schema where the view will written to
        :param join_type: the type of join
        """

        (self.schema_a, self.table_a) = table_parts(table_a)
        (self.schema_b, self.table_b) = table_parts(table_b)

        self.join_type = join_type

        if output_schema is None:
            if self.schema_a != self.schema_b:
                raise ValueError('Destination schema cannot be guessed if different on sources tables.')
            else:
                self.output_schema = self.schema_a
        else:
            self.output_schema = output_schema

        self.output_view = "vw_{ta}_{tb}".format(ta=self.table_a, tb=self.table_b)

        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cur = self.conn.cursor()

        self.a_cols = columns(self.cur, self.schema_a, self.table_a)
        self.b_cols = columns(self.cur, self.schema_b, self.table_b)
        (self.ref_a_key, self.ref_b_key) = reference_columns(self.cur, self.schema_a, self.table_a, self.schema_b, self.table_b)
        self.b_pkey = primary_key(self.cur, self.schema_b, self.table_b)
        self.b_cols_wo_pkey = columns(self.cur, self.schema_b, self.table_b, True)


    def create(self) -> bool:
        """

        :return:
        """
        sql = self.__view()
        sql += self.__insert_trigger()

        self.cur.execute(sql)
        self.conn.commit()

        print(sql)

        return True

    def __view(self) -> str:
        """
        Create the SQL code for the view
        :return: the SQL code
        """

        sql = "CREATE OR REPLACE VIEW {ds}.{dt} AS SELECT \n".format(ds=self.output_schema, dt=self.output_view)
        sql += ", ".join(self.a_cols)
        if len(self.b_cols_wo_pkey):
            sql += ', ' + ', '.join(self.b_cols_wo_pkey) + '\n'
        sql += " FROM {sa}.{ta} \n" \
               " {jt} JOIN {sb}.{tb} ON {tb}.{rbk} = {ta}.{rak};\n" \
            .format(jt=self.join_type.value,
                    sa=self.schema_a,
                    ta=self.table_a,
                    rak=self.ref_a_key,
                    sb=self.schema_b,
                    tb=self.table_b,
                    rbk=self.ref_b_key)
        return sql


    def __insert_trigger(self) -> str:
        """

        :return:
        """

        sql = "CREATE OR REPLACE FUNCTION {ds}.tr_{dt}_insert() RETURNS trigger AS\n" \
              "$BODY$\n" \
              "BEGIN\n" \
              "INSERT INTO {sb}.{tb}\n" \
              "    ( {b_cols} )\n" \
              "  VALUES (\n" \
              "    COALESCE( NEW.{bpk}, {bkp_def} ),\n" \
              "    {b_new_cols}\n" \
              "  )\n" \
              "  RETURNING {bpk} INTO NEW.{rak};\n" \
              "INSERT INTO {sa}.{ta} ( {a_cols} )\n" \
              "  VALUES ( {a_new_cols} );\n" \
              "RETURN NEW;\n" \
              "END; $BODY$\n" \
              "LANGUAGE plpgsql;\n\n" \
              "CREATE TRIGGER tr_{dt}_on_insert\n" \
              "  INSTEAD OF INSERT ON {ds}.{dt}" \
              "  FOR EACH ROW EXECUTE PROCEDURE {ds}.tr_{dt}_insert();"\
            .format(ds=self.output_schema,
                    dt=self.output_view,
                    sa=self.schema_a,
                    ta=self.table_a,
                    rak=self.ref_a_key,
                    a_cols=list2str(self.a_cols),
                    a_new_cols=list2str(self.a_cols, prepend='NEW.'),
                    sb=self.schema_b,
                    tb=self.table_b,
                    b_cols=list2str(self.b_cols),
                    bpk=self.b_pkey,
                    bkp_def=default_value(self.cur, self.schema_b, self.table_a, self.b_pkey),
                    b_new_cols=list2str(self.b_cols_wo_pkey, prepend='NEW.'))

        return sql
