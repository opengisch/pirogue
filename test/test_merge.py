#! /usr/bin/env python

import unittest
import yaml
import psycopg2
import psycopg2.extras
from pirogue.merge import Merge

pg_service = 'pirogue_test'


class TestMerge(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.conn.close()

    def test_simple(self):
        yaml_definition = yaml.safe_load(open("test/merge_simple.yaml"))
        Merge(yaml_definition, pg_service=pg_service).create()

    def test_sql_definition(self):
        yaml_definition = yaml.safe_load(open("test/merge_sql_def.yaml"))
        Merge(yaml_definition, pg_service=pg_service).create()

    def test_custom_order(self):
        yaml_definition = yaml.safe_load(open("test/merge_columns_order.yaml"))
        Merge(yaml_definition, pg_service=pg_service).create()

        check_columns = """
            select c.column_name
            from information_schema.tables t
                left join information_schema.columns c
                          on t.table_schema = c.table_schema
                          and t.table_name = c.table_name
            where table_type = 'VIEW'
                  and t.table_schema = 'pirogue_test'
                  and t.table_name = 'ordered_view'
            order by ordinal_position;
        """
        self.cur.execute(check_columns)
        cols = self.cur.fetchall()
        self.assertEqual(cols[0][0], 'my_custom_type', 'Custom type is failing')
        self.assertEqual(cols[1][0], 'eye_color_renamed', 'Columns on top is failing')
        self.assertEqual(cols[-1][0], 'fk_breed', 'Columns on top is failing')



if __name__ == '__main__':
    unittest.main()
