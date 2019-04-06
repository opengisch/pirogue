# -*- coding: utf-8 -*-


class Join:
    """
    Creates a simple join view with associated triggers to edit.
    """

    def __init__(self, pg_service: str, table_a: str, table_b: str):
        """
        Produces the SQL code of the join table and triggers
        :param pg_service:
        :param table_a:
        :param table_b:
        """
        self.pg_service = pg_service
        self.table_a = table_a
        self.table_b = table_b

    def create(self) -> bool:
        """

        :return:
        """


        return False

