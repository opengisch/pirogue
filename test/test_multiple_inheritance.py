#! /usr/bin/env python

import unittest
import yaml
import psycopg2
import psycopg2.extras
from pirogue import MultipleInheritance
from pirogue.utils import default_value
from pirogue.exceptions import InvalidDefinition

pg_service = 'pirogue_test'


class TestMultipleInheritance(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cur = self.conn.cursor()

        sql = open("test/demo_data.sql").read()
        self.cur.execute(sql)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_insert_update_delete(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        MultipleInheritance(yaml_definition, pg_service=pg_service).create()

        # insert
        self.cur.execute("INSERT INTO pirogue_test.vw_merge_animal (animal_type,name,year,fk_cat_breed,eye_color) VALUES ('cat','felix',1985,2,'black');")
        self.cur.execute("SELECT animal_type, year, fk_cat_breed FROM pirogue_test.vw_merge_animal WHERE name = 'felix';")
        res = self.cur.fetchone()
        self.assertEqual(res[0], 'cat')
        self.assertEqual(res[1], 1985)
        self.assertEqual(res[2], 2)
        # update
        self.cur.execute("SELECT * FROM pirogue_test.cat WHERE eye_color = 'black';")
        self.assertIsNotNone(self.cur.fetchone())
        self.cur.execute("UPDATE pirogue_test.vw_merge_animal SET animal_type = 'dog' WHERE name = 'felix';")
        self.cur.execute("SELECT * FROM pirogue_test.cat WHERE eye_color = 'black';")
        self.assertIsNone(self.cur.fetchone())
        # delete
        self.cur.execute("SELECT * FROM pirogue_test.vw_merge_animal WHERE name = 'felix';")
        self.assertIsNotNone(self.cur.fetchone())
        self.cur.execute("DELETE FROM pirogue_test.vw_merge_animal WHERE name = 'felix';")
        self.cur.execute("SELECT * FROM pirogue_test.vw_merge_animal WHERE name = 'felix';")
        self.assertIsNone(self.cur.fetchone())

    def test_type_change_not_allowed(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        yaml_definition['view_name'] = 'vw_animal_no_type_change'
        yaml_definition['allow_type_change'] = False
        MultipleInheritance(yaml_definition, pg_service=pg_service).create()
        self.cur.execute("INSERT INTO pirogue_test.vw_animal_no_type_change (animal_type,name,year,fk_cat_breed,eye_color) VALUES ('dog','albert',1933,2,'yellow');")
        error_caught = False
        try:
            self.cur.execute("UPDATE pirogue_test.vw_animal_no_type_change SET animal_type = 'cat';")
        except psycopg2.errors.RaiseException:
            error_caught = True
        self.assertTrue(error_caught)

    def test_invalid_definition(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        yaml_definition['MyBadKey'] = 'Ouch'
        error_caught = False
        try:
            MultipleInheritance(yaml_definition, pg_service=pg_service).create()
        except InvalidDefinition:
            error_caught = True
            self.assertTrue(error_caught)

    def test_pkey_default_value(self):
        yaml_definition = yaml.safe_load(open("test/multiple_inheritance.yaml"))
        yaml_definition['pkey_default_value'] = True
        MultipleInheritance(yaml_definition, pg_service=pg_service).create()
        self.assertEqual(default_value(self.cur, 'pirogue_test', 'animal', 'aid'), default_value(self.cur, 'pirogue_test', 'vw_merge_animal', 'aid'))




if __name__ == '__main__':
    unittest.main()
