# -*- coding: utf-8 -*-

import os
import psycopg2
import psycopg2.extras

from pirogue.utils import table_parts, list2str, update_columns
from pirogue.information_schema import TableHasNoPrimaryKey, NoReferenceFound, \
    columns, reference_columns, primary_key, default_value


class ReferencedTableDefinedBeforeReferencing(Exception):
    pass


class Merge:
    """
    Creates a simple join view with associated triggers to edit.
    """

    def __init__(self, definition: dict, pg_service: str = None, variables: dict = {}):
        """
        Produces the SQL code of the join table and triggers
        :param definition: the YAML definition of the merge view
        :param pg_service:
        :param variables: dictionary for variables to be used in SQL deltas ( name => value )
        """
        if pg_service is None:
            pg_service = os.getenv('PGSERVICE')

        self.variables = variables

        (self.master_schema, self.master_table) = table_parts(definition['table'])

        # global options:
        self.fkey_is_pkey = definition.get('fkey_is_pkey', False)

        self.view_schema = definition.get('view_schema', self.master_schema)
        self.view_name = definition.get('view_name', "vw_merge_{t}".format(t=self.master_table))
        self.view_alias = definition.get('alias', self.master_table)
        self.short_alias = definition.get('short_alias', self.view_alias)
        self.type_name = definition.get('type_name', '{al}_type'.format(al=self.view_alias))

        self.conn = psycopg2.connect("service={0}".format(pg_service))
        self.cursor = self.conn.cursor()

        # columns are required if the definition is not a table but SELECT request.
        # with PG 11+ we could use \gdesc to find out the columns out of the request.
        # see https://www.depesz.com/2017/09/21/waiting-for-postgresql-11-add-gdesc-psql-command/
        self.master_cols = definition.get('columns', None) or columns(self.cursor, self.master_schema, self.master_table)

        # create a dictionnary so it can be appended to joined tables
        self.main_table_def = {self.view_alias: {'cols': self.master_cols,
                                                 'cols_wo_ref_key': self.master_cols,
                                                 'short_alias': self.short_alias,
                                                 'columns_on_top': definition.get('columns_on_top', [])}}

        try:
            self.master_pkey = definition.get('key', None) or primary_key(self.cursor, self.master_schema, self.master_table)
        except TableHasNoPrimaryKey:
            raise TableHasNoPrimaryKey('{vn} has no primary key, specify it with "key"'.format(vn=self.view_alias))

        self.joins = definition['joins']
        self.joined_ref_master_key = []
        for alias, table_def in self.joins.items():
            (table_def['table_schema'], table_def['table_name']) = table_parts(table_def['table'])
            table_def['short_alias'] = table_def.get('short_alias', alias)
            table_def['cols'] = table_def.get('only_columns', None) or columns(self.cursor, table_def['table_schema'],
                                                                               table_def['table_name'],
                                                                               skip_columns=table_def.get('skip_columns', []))

            if 'fkey' in table_def:
                table_def['ref_master_key'] = table_def['fkey']
            else:
                try:
                    table_def['ref_master_key'] = reference_columns(self.cursor,
                                                                    table_def['table_schema'], table_def['table_name'],
                                                                    self.master_schema, self.master_table)[0]
                except NoReferenceFound as e:
                    # no reference found (this is probably because using a SQL definition instead of a proper table
                    # defaulting to primary key if allowed by global option
                    if self.fkey_is_pkey:
                        try:
                            table_def['ref_master_key'] = primary_key(self.cursor, table_def['table_schema'], table_def['table_name'])
                        except TableHasNoPrimaryKey:
                            raise NoReferenceFound('{ts}.{tn} has no reference to {ms}.{mt}'.format(ts=table_def['table_schema'],
                                                                                                    tn=table_def['table_name'],
                                                                                                    ms=self.master_schema,
                                                                                                    mt=self.master_table))
                    else:
                        raise e
            try:
                table_def['pkey'] = primary_key(self.cursor, table_def['table_schema'], table_def['table_name'])
            except TableHasNoPrimaryKey:
                table_def['pkey'] = table_def['ref_master_key']

            # make a copy, otherwise keeps reference
            table_def['cols_wo_ref_key'] = list(table_def['cols'])
            if table_def['ref_master_key'] in table_def['cols_wo_ref_key']:
              table_def['cols_wo_ref_key'].remove(table_def['ref_master_key'])

            # fix lower levels references (table not linked to the master table)
            if 'referenced_by' in table_def:
                table_def['reference_master_table'] = table_def.get('is_type', False)
                table_def['referenced_by_alias'] = self.joins[table_def['referenced_by']]['short_alias']
                try:
                    table_def['referenced_by_key'] = table_def.get('referenced_by_key', None) or \
                                                     reference_columns(self.cursor,
                                                                       self.joins[table_def['referenced_by']]['table_schema'],
                                                                       self.joins[table_def['referenced_by']]['table_name'],
                                                                       table_def['table_schema'], table_def['table_name'])[0]
                except NoReferenceFound as e:
                    if self.fkey_is_pkey:
                        table_def['referenced_by_key'] = self.joins[table_def['referenced_by']]['pkey']
                    else:
                        raise e
                except KeyError:
                    raise ReferencedTableDefinedBeforeReferencing('"{rd}" should be defined after "{rg}"'.format(rd=alias,
                                                                                                                 rg=table_def['referenced_by']))
            else:
                table_def['reference_master_table'] = table_def.get('is_type', True)
                table_def['referenced_by_alias'] = self.view_alias
                table_def['referenced_by_key'] = table_def.get('referenced_by_key', None) or self.master_pkey


        # print(list2str([ alias+' '+col   for alias, table_def in self.joins.items()
        #                      for col in table_def['cols_wo_ref_key']], prepend='\n'))
        # raise Exception
        # print(list2str( [alias+' '+col
        #                  for (alias, col)
        #                  in sorted(
        #                     [ (alias,col) for alias, table_def in self.joins.items()
        #                                   for col in table_def['cols_wo_ref_key']
        #                     ]
        #                     , key=lambda x: (0 if x[1] in self.joins[x[0]].get('columns_on_top', []) else 1,
        #                                      list(self.joins.keys()).index(x[0]),
        #                                      list(self.joins[x[0]]['cols']).index(x[1]))
        #                     )],
        #                     prepend='\n'))
        # raise Exception


    def create(self) -> bool:
        """

        :return:
        """
        for sql in [self.__view()]:
            print(sql)
            try:
                if self.variables:
                    self.cursor.execute(sql, self.variables)
                else:
                    self.cursor.execute(sql)
            except psycopg2.Error as e:
                print("*** Failing:\n{}\n***".format(sql))
                raise e
        self.conn.commit()
        return True

    def column_alias(self, table_def: dict, column: str, prepend_as: bool = False) -> str:
        """

        :param table_alias:
        :param column:
        :param prepend_as:
        :return: empty string if there is no alias (i.e = field name)
        """
        col_alias = ""
        remap_dict = table_def.get('remap_columns', {})
        prefix = table_def.get('prefix', None)
        if column in remap_dict:
            col_alias = remap_dict[column]
        elif prefix:
            col_alias = prefix + column
        if prepend_as and col_alias:
            col_alias = ' AS {al}'.format(al=col_alias)
        return col_alias

    def __view(self) -> str:
        """
        :return:
        """

        sql = """
CREATE OR REPLACE VIEW {vs}.{vn} AS
  SELECT
    CASE
      {type}
      ELSE 'unknown'::text
    END AS {type_name}{columns}
  FROM {ms}.{mt} {alias}
    {joined_tables};
                
        """.format(vs=self.view_schema,
                   vn=self.view_name,
                   type=list2str(elements=["WHEN {shal}.{mrf} IS NOT NULL THEN '{al}'::text"
                                           .format(shal=table_def['short_alias'], mrf=table_def['ref_master_key'],
                                                   al=alias)
                                           for alias, table_def in self.joins.items() if table_def['reference_master_table']],
                                 sep='\n      '),
                   type_name=self.type_name,
                   #master_cols=list2str(self.master_cols, prepend='\n    {al}.'.format(al=self.view_alias), prepend_to_list=','),
                   columns=list2str(['{table_alias}.{column}{col_alias}'.format(table_alias=table_def['short_alias'],
                                                                                    column=col,
                                                                                    col_alias=self.column_alias(table_def, col, prepend_as=True))
                                         for (alias, table_def, col)
                                         in sorted(
                           [(alias, table_def, col) for alias, table_def in {**self.main_table_def, **self.joins}.items()
                            for col in table_def['cols_wo_ref_key']
                            ]
                           , key=lambda x: (0 if x[2] in x[1].get('columns_on_top', []) else 1,
                                            list(self.joins.keys()).index(x[0])+1 if x[0] in self.joins else 0,
                                            x[1]['cols'].index(x[2]))
                       )],
                                        prepend='\n    ', prepend_to_list=','),
                   ms=self.master_schema,
                   mt=self.master_table,
                   alias=self.view_alias,
                   joined_tables=list2str(elements=["LEFT JOIN {tb} {al} ON {al}.{rmk} = {rba}.{mpk}"
                                                    .format(al=table_def['short_alias'],
                                                            tb=table_def['table'],
                                                            rmk=table_def['ref_master_key'],
                                                            rba=table_def['referenced_by_alias'],
                                                            mpk=table_def['referenced_by_key']) for table_def in self.joins.values()],
                                          sep='\n    ')
                   )
        return sql

 #        "CREATE OR REPLACE VIEW qgep_od.vw_qgep_wastewater_structure AS
 # SELECT ws.identifier,
 #        CASE
 #            WHEN ma.obj_id IS NOT NULL THEN 'manhole'::text
 #            WHEN ss.obj_id IS NOT NULL THEN 'special_structure'::text
 #            WHEN dp.obj_id IS NOT NULL THEN 'discharge_point'::text
 #            WHEN ii.obj_id IS NOT NULL THEN 'infiltration_installation'::text
 #            ELSE 'unknown'::text
 #        END AS ws_type,
 #    ma.function AS ma_function,
 #    ss.function AS ss_function,
 #
 #   FROM ( SELECT ws_1.obj_id,
 #            st_collect(co.situation_geometry)::geometry(MultiPointZ,2056) AS situation_geometry,
 #                CASE
 #                    WHEN count(wn_1.obj_id) = 1 THEN min(wn_1.obj_id::text)
 #                    ELSE NULL::text
 #                END AS wn_obj_id
 #           FROM qgep_od.wastewater_structure ws_1
 #             FULL JOIN qgep_od.structure_part sp ON sp.fk_wastewater_structure::text = ws_1.obj_id::text
 #             LEFT JOIN qgep_od.cover co ON co.obj_id::text = sp.obj_id::text
 #             RIGHT JOIN qgep_od.wastewater_networkelement ne ON ne.fk_wastewater_structure::text = ws_1.obj_id::text
 #             RIGHT JOIN qgep_od.wastewater_node wn_1 ON wn_1.obj_id::text = ne.obj_id::text
 #          GROUP BY ws_1.obj_id) aggregated_wastewater_structure
 #     LEFT JOIN qgep_od.wastewater_structure ws ON ws.obj_id::text = aggregated_wastewater_structure.obj_id::text
 #     LEFT JOIN qgep_od.cover main_co ON main_co.obj_id::text = ws.fk_main_cover::text
 #     LEFT JOIN qgep_od.structure_part main_co_sp ON main_co_sp.obj_id::text = ws.fk_main_cover::text
 #     LEFT JOIN qgep_od.manhole ma ON ma.obj_id::text = ws.obj_id::text
 #     LEFT JOIN qgep_od.special_structure ss ON ss.obj_id::text = ws.obj_id::text
 #     LEFT JOIN qgep_od.discharge_point dp ON dp.obj_id::text = ws.obj_id::text
 #     LEFT JOIN qgep_od.infiltration_installation ii ON ii.obj_id::text = ws.obj_id::text
 #     LEFT JOIN qgep_od.vw_wastewater_node wn ON wn.obj_id::text = aggregated_wastewater_structure.wn_obj_id;"

    def __view2(self) -> str:
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
                    a_new_cols=list2str(
                        self.a_cols, prepend='\n    NEW.', append=''),
                    sb=self.schema_b,
                    tb=self.table_b,
                    b_cols=list2str(self.b_cols, prepend='\n    '),
                    bpk=self.b_pkey,
                    bkp_def=default_value(
                        self.cursor, self.schema_b, self.table_b, self.b_pkey),
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
