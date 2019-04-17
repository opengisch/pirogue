# -*- coding: utf-8 -*-

import os
import psycopg2
import psycopg2.extras

from pirogue.utils import table_parts, list2str, update_columns
from pirogue.information_schema import TableHasNoPrimaryKey, columns, reference_columns, primary_key, default_value



class Merge:
    """
    Creates a simple join view with associated triggers to edit.
    """

    def __init__(self, yaml_definition: dict, pg_service: str=None):
        """
        Produces the SQL code of the join table and triggers
        :param yaml_definition: the YAML definition of the merge view
        :param pg_service:
        """
        if pg_service is None:
            pg_service = os.getenv('PGSERVICE')

        print('global def:', yaml_definition)
        print('***')

        (self.master_schema, self.master_table) = table_parts(yaml_definition['table'])

        self.view_schema = yaml_definition.get('view_schema', self.master_schema)
        self.view_name = yaml_definition.get('view_name', "vw_merge_{t}".format(t=self.master_table))

        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cur = self.conn.cursor()

        self.master_cols = columns(self.cur, self.master_schema, self.master_table)

        self.master_pkey = primary_key(self.cur, self.master_schema, self.master_table)
        self.master_cols_wo_pkey = columns(self.cur, self.master_schema, self.master_table, True)

        self.joins = yaml_definition['joins']
        for alias, table_def in self.joins.items():
            (table_def['table_schema'], table_def['table_name']) = table_parts(table_def['table'])
            table_def['cols'] = columns(self.cur, table_def['table_schema'], table_def['table_name'])
            try:
                table_def['pkey'] = primary_key(self.cur, table_def['table_schema'], table_def['table_name'])
            except TableHasNoPrimaryKey:
                (table_def['pkey'], _) = reference_columns(self.cur, table_def['table_schema'], table_def['table_name'],
                                                        self.master_schema, self.master_table)

            table_def['cols_wo_pkey'] = list(table_def['cols'])  # make a copy, otherwise keeps reference
            table_def['cols_wo_pkey'].remove(table_def['pkey'])

            print(table_def)

            TODO: test with serial id for joins and here after

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
              "INSERT INTO {sb}.{tb} ( {b_cols} )\n" \
              "  VALUES (\n" \
              "    COALESCE( NEW.{rak}, {bkp_def} ), {b_new_cols} )\n" \
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
                    a_cols=list2str(self.a_cols, prepend='\n    '),
                    a_new_cols=list2str(self.a_cols, prepend='\n    NEW.', append=''),
                    sb=self.schema_b,
                    tb=self.table_b,
                    b_cols=list2str(self.b_cols, prepend='\n    '),
                    bpk=self.b_pkey,
                    bkp_def=default_value(self.cur, self.schema_b, self.table_b, self.b_pkey),
                    b_new_cols=list2str(self.b_cols_wo_pkey, prepend='\n    NEW.', append=''))
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
                    a_up_cols=update_columns(self.a_cols_wo_pkey, sep='\n    , '),
                    sb=self.schema_b,
                    tb=self.table_b,
                    bpk=self.b_pkey,
                    rak=self.ref_a_key,
                    b_up_cols=update_columns(self.b_cols_wo_pkey, sep='\n    , '))
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
