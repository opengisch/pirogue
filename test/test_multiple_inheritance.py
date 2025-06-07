#! /usr/bin/env python

import unittest

import psycopg
import yaml

from pirogue import MultipleInheritance
from pirogue.exceptions import InvalidDefinition
from pirogue.utils import default_value

pg_service = "pirogue_test"


class TestMultipleInheritance(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(f"service={pg_service}")

        sql = open("test/demo_data.sql").read()
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_insert_update_delete(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        MultipleInheritance(definition=yaml_definition, connection=self.conn).create(commit=True)

        cur = self.conn.cursor()

        # insert
        cur.execute(
            "INSERT INTO pirogue_test.vw_merge_animal (animal_type,name,year,fk_cat_breed,eye_color) VALUES ('cat','felix',1985,2,'black');"
        )
        cur.execute(
            "SELECT animal_type, year, fk_cat_breed FROM pirogue_test.vw_merge_animal WHERE name = 'felix';"
        )
        res = cur.fetchone()
        self.assertEqual(res[0], "cat")
        self.assertEqual(res[1], 1985)
        self.assertEqual(res[2], 2)
        # update
        cur.execute("SELECT * FROM pirogue_test.cat WHERE eye_color = 'black';")
        self.assertIsNotNone(cur.fetchone())
        cur.execute(
            "UPDATE pirogue_test.vw_merge_animal SET animal_type = 'dog' WHERE name = 'felix';"
        )
        cur.execute("SELECT * FROM pirogue_test.cat WHERE eye_color = 'black';")
        self.assertIsNone(cur.fetchone())
        # delete
        cur.execute("SELECT * FROM pirogue_test.vw_merge_animal WHERE name = 'felix';")
        self.assertIsNotNone(cur.fetchone())
        cur.execute("DELETE FROM pirogue_test.vw_merge_animal WHERE name = 'felix';")
        cur.execute("SELECT * FROM pirogue_test.vw_merge_animal WHERE name = 'felix';")
        self.assertIsNone(cur.fetchone())

    def test_type_change_not_allowed(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        yaml_definition["view_name"] = "vw_animal_no_type_change"
        yaml_definition["allow_type_change"] = False
        MultipleInheritance(definition=yaml_definition, connection=self.conn).create(commit=True)
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO pirogue_test.vw_animal_no_type_change (animal_type,name,year,fk_cat_breed,eye_color) VALUES ('dog','albert',1933,2,'yellow');"
        )
        error_caught = False
        try:
            cur.execute("UPDATE pirogue_test.vw_animal_no_type_change SET animal_type = 'cat';")
        except psycopg.errors.RaiseException:
            error_caught = True
        self.assertTrue(error_caught)

    def test_invalid_definition(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        yaml_definition["MyBadKey"] = "Ouch"
        error_caught = False
        try:
            MultipleInheritance(definition=yaml_definition, connection=self.conn).create()
        except InvalidDefinition:
            error_caught = True
            self.assertTrue(error_caught)

    def test_pkey_default_value(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        yaml_definition["pkey_default_value"] = True
        MultipleInheritance(definition=yaml_definition, connection=self.conn).create()
        self.assertEqual(
            default_value(self.conn, "pirogue_test", "animal", "aid"),
            default_value(self.conn, "pirogue_test", "vw_merge_animal", "aid"),
        )

    def test_merge_no_columns(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        yaml_definition["joins"]["aardvark"]["skip_columns"] = ["aid", "father"]
        MultipleInheritance(definition=yaml_definition, connection=self.conn).create()


if __name__ == "__main__":
    unittest.main()
