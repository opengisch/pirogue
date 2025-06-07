#! /usr/bin/env python

import unittest

import psycopg
import yaml

from pirogue import MultipleInheritance, SimpleJoins
from pirogue.exceptions import InvalidDefinition

pg_service = "pirogue_test"


class TestSimpleJoins(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(f"service={pg_service}")

        sql = open("test/demo_data.sql").read()
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_simple(self):
        yaml_definition = yaml.safe_load(open("test/simple_joins.yaml"))
        SimpleJoins(yaml_definition, connection=self.conn).create()

    def test_based_on_view(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        MultipleInheritance(definition=yaml_definition, connection=self.conn).create()
        yaml_definition = yaml.safe_load(open("test/simple_joins_based_on_view.yaml"))
        SimpleJoins(definition=yaml_definition, connection=self.conn).create()

    def test_invalid_definition(self):
        yaml_definition = yaml.safe_load(open("test/simple_joins.yaml"))
        yaml_definition["MyBadKey"] = "Ouch"
        error_caught = False
        try:
            SimpleJoins(definition=yaml_definition, connection=self.conn).create()
        except InvalidDefinition:
            error_caught = True
        self.assertTrue(error_caught)


if __name__ == "__main__":
    unittest.main()
