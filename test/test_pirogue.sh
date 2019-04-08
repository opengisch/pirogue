#! /usr/bin/env bash

set -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export PYTHONPATH=${DIR}/..:${PYTHONPATH}

export PGSERVICE=pirogue_test

${DIR}/../scripts/pirogue join pirogue_test.cat pirogue_test.animal
psql -v ON_ERROR_STOP="on" -c "INSERT into pirogue_test.vw_cat_animal (fk_breed, eye_color, name, year) VALUES (1, 'yellow', 'ninja', 1934);"
psql -v ON_ERROR_STOP="on" -c "UPDATE pirogue_test.vw_cat_animal SET eye_color = 'black', year = 2000 WHERE cid = 1;"

