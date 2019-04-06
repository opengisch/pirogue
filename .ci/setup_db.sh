#!/usr/bin/env bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PGUSER=postgres

printf "[pirogue_test]\nhost=localhost\ndbname=${pirogue_test_db}\nuser=postgres\n\n" >> ~/.pg_service.conf
dropdb --if-exists pirogue_test_db
createdb pirogue_test_db


PGSERVICE=pirogue_test psql --quiet -v ON_ERROR_STOP=on -f ${DIR}/data/demo_data.sql;
