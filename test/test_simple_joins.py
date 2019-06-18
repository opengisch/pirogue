#! /usr/bin/env python

import unittest
import yaml
import psycopg2
import psycopg2.extras
from pirogue import SimpleJoins, MultipleInheritance
from pirogue.exceptions import InvalidDefinition

pg_service = 'pirogue_test'


class TestSimpleJoins(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cur = self.conn.cursor()

        sql = open("test/demo_data.sql").read()
        self.cur.execute(sql)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_simple(self):
        yaml_definition = yaml.safe_load(open("test/simple_joins.yaml"))
        SimpleJoins(yaml_definition, pg_service=pg_service).create()

    def test_based_on_view(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        MultipleInheritance(yaml_definition, pg_service=pg_service).create()
        yaml_definition = yaml.safe_load(open("test/simple_joins_based_on_view.yaml"))
        SimpleJoins(yaml_definition, pg_service=pg_service).create()

    def test_invalid_definition(self):
        yaml_definition = yaml.safe_load(open("test/simple_joins.yaml"))
        yaml_definition['MyBadKey'] = 'Ouch'
        error_caught = False
        try:
            SimpleJoins(yaml_definition, pg_service=pg_service).create()
        except InvalidDefinition:
            error_caught = True
        self.assertTrue(error_caught)


if __name__ == '__main__':
    unittest.main()
