# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras

from pirogue.core.utils import table_parts

class Join:
    """
    Creates a simple join view with associated triggers to edit.
    """

    def __init__(self, table_a: str, table_b: str, pg_service: str=None, destination_schema: str=None):
        """
        Produces the SQL code of the join table and triggers
        :param table_a:
        :param table_b:
        :param pg_service:
        :param destination_schema: the schema where the view will written to
        """

        (self.schema_a, self.table_a) = table_parts(table_a)
        (self.schema_b, self.table_b) = table_parts(table_b)

        if self.schema_a != self.schema_b:
            if destination_schema is None:
                raise ValueError('Destination schema cannot be guessed if different on sources tables.')
            self.destination_schema = destination_schema
        else:
            self.destination_schema = self.schema_a

        self.destination_table = 'vw_{ta}_{tb}'.format(ta=self.table_a, tb=self.table_b)

        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cur = self.conn.cursor()

    def create(self) -> bool:
        """

        :return:
        """
        sql = self.__view()

        return True

    def __view(self) -> str:
        """
        Create the SQL code for the view
        :return: the SQL code
        """
        sql = "CREATE OR REPLACE VIEW {ds}.{dt}".format(ds=self.destination_schema, dt=self.destination_table)
        return True

