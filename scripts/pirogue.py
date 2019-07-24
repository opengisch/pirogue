#!/usr/bin/env python3

import argparse
import os
import yaml

from pirogue.single_inheritance import SingleInheritance
from pirogue.multiple_inheritance import MultipleInheritance
from pirogue.simple_joins import SimpleJoins


def main():

    # create the top-level parser
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--version", help="print the version and exit", action='store_true')

    subparsers = parser.add_subparsers(title='commands', description='pirogue command', dest='command')

    # single inheritance view
    single_inheritance_parser = subparsers.add_parser('single_inheritance', help='create a single inheritance view')
    single_inheritance_parser.add_argument('parent_table')
    single_inheritance_parser.add_argument('child_table')
    single_inheritance_parser.add_argument('-s', '--view-schema', help='schema for the created join view')
    single_inheritance_parser.add_argument('-v', '--view-name', help='name for the created join view')
    single_inheritance_parser.add_argument('-d', '--pkey-default-value', action='store_true',
                                           help='The primary key column of the view will have a default value'
                                                ' according to the child primary key table')
    single_inheritance_parser.add_argument('-p', '--pg_service', help='postgres service')


    # multiple inheritance view
    multiple_inheritance_parser = subparsers.add_parser('multiple_inheritance', help='create a multiple inheritance view')
    multiple_inheritance_parser.add_argument('definition_file', help='YAML definition of the merge view', type=argparse.FileType('r'))
    multiple_inheritance_parser.add_argument('-j', '--create-joins', action='store_true',
                                             help='Create simple join view for all joined tables.')
    multiple_inheritance_parser.add_argument('-d', '--drop', action='store_true',
                                             help='Drop existing views, type and triggers.')
    multiple_inheritance_parser.add_argument('-v', '--var', nargs=3,
                                             help='Assign variable for running SQL deltas. '
                                                  'Format is: (string|float|int) name value. ',
                                             action='append', default=[])
    multiple_inheritance_parser.add_argument('-p', '--pg_service', help='postgres service')


    # multiple inheritance view
    simple_joins = subparsers.add_parser('simple_joins', help='create a view for simple joins without any editing capability')
    simple_joins.add_argument('definition_file', help='YAML definition of the merge view', type=argparse.FileType('r'))
    simple_joins.add_argument('-p', '--pg_service', help='postgres service')

    args = parser.parse_args()

    # print the version and exit
    if args.version:
        import pkg_resources
        print('pirogue version: {}'.format(pkg_resources.get_distribution('pirogue').version))
        parser.exit()

    # if no command is passed, print the help and exit
    if not args.command:
        parser.print_help()
        parser.exit()

    exit_val = 0

    if args.pg_service:
        pg_service = args.pg_service
    else:
        pg_service = os.getenv('PGSERVICE')

    if args.command == 'single_inheritance':
        success = SingleInheritance(parent_table=args.parent_table,
                                    child_table=args.child_table,
                                    pg_service=pg_service,
                                    view_schema=args.view_schema,
                                    view_name=args.view_name,
                                    pkey_default_value=args.pkey_default_value).create()
        if not success:
            exit_val = 1

    elif args.command == 'multiple_inheritance':
        yaml_definition = yaml.safe_load(args.definition_file)
        variables = {}
        for v in args.var or ():
            if v[0] == 'float':
                variables[v[1]] = float(v[2])
            elif v[0] == 'int':
                variables[v[1]] = int(v[2])
            else:
                variables[v[1]] = v[2]
        MultipleInheritance(yaml_definition,
                            variables=variables,
                            create_joins=args.create_joins,
                            pg_service=pg_service).create()

    elif args.command == 'simple_joins':
        yaml_definition = yaml.safe_load(args.definition_file)
        SimpleJoins(yaml_definition, pg_service=pg_service).create()


    exit(exit_val)


if __name__ == "__main__":
    main()
