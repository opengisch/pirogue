# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras

from enum import Enum

from pirogue.utils import table_parts
from pirogue.information_schema import columns, reference_columns


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

        self.output_view = 'vw_{ta}_{tb}'.format(ta=self.table_a, tb=self.table_b)

        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cur = self.conn.cursor()


    def create(self) -> bool:
        """

        :return:
        """
        sql = self.__view()

        print(self.cur.execute(sql))
        print(self.conn.commit())

        print(sql)

        return True

    def __view(self) -> str:
        """
        Create the SQL code for the view
        :return: the SQL code
        """
        a_cols = columns(self.cur, self.schema_a, self.table_a)
        b_cols = columns(self.cur, self.schema_b, self.table_b, True)

        (ref_a_key, ref_b_key) = reference_columns(self.cur, self.schema_a, self.table_a, self.schema_b, self.table_b)

        sql = "CREATE OR REPLACE VIEW {ds}.{dt} AS\n".format(ds=self.output_schema, dt=self.output_view)
        sql += "  SELECT "

        sql += ', '.join(a_cols)

        if len(b_cols):
            sql += ', ' + ', '.join(b_cols) + '\n'
        sql += "  FROM {sa}.{ta} \n" \
               " {jt} JOIN {sb}.{tb} ON {tb}.{rbk} = {ta}.{rak};\n" \
            .format(jt=self.join_type.value,
                    sa=self.schema_a,
                    ta=self.table_a,
                    rak=ref_a_key,
                    sb=self.schema_b,
                    tb=self.table_b,
                    rbk=ref_b_key)


        return sql

