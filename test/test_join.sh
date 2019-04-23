#! /usr/bin/env bash

set -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export PYTHONPATH=${DIR}/..:${PYTHONPATH}

export PGSERVICE=pirogue_test

${DIR}/../scripts/pirogue join pirogue_test.animal pirogue_test.cat
${DIR}/../scripts/pirogue join pirogue_test.animal pirogue_test.aardvark --view-name vw_aardvark
${DIR}/../scripts/pirogue join pirogue_test.animal pirogue_test.eagle --pkey-default-value
ERROR=0

PSQL_ARGS="--tuples-only --no-align --field-separator @"

echo "test simple insert"
psql --quiet -v ON_ERROR_STOP="on" -c "INSERT into pirogue_test.vw_animal_cat (cid, fk_breed, eye_color, name, year) VALUES (12345, '1', 'yellow', 'ninja', 1934);"
RESULT=$(psql ${PSQL_ARGS} -c "SELECT cid,fk_breed,eye_color,name,year FROM pirogue_test.vw_animal_cat")
EXPECTED=12345@1@yellow@ninja@1934
if [[ ${RESULT} =~ "${EXPECTED}" ]]; then echo "ok"; else echo "*** ERROR expected result: ${EXPECTED} got ${RESULT}" && ERROR=1; fi

echo "test update trigger"
psql --quiet -v ON_ERROR_STOP="on" -c "UPDATE pirogue_test.vw_animal_cat SET eye_color = 'black', year = 2000 WHERE name = 'ninja';"
RESULT=$(psql ${PSQL_ARGS} -c "SELECT eye_color,year FROM pirogue_test.vw_animal_cat")
EXPECTED=black@2000
if [[ ${RESULT} =~ "${EXPECTED}" ]]; then echo "ok"; else echo "*** ERROR expected result: ${EXPECTED} got ${RESULT}" && ERROR=1; fi

echo "test delete trigger"
psql --quiet -v ON_ERROR_STOP="on" -c "DELETE FROM pirogue_test.vw_animal_cat WHERE eye_color = 'black';"
RESULT=$(psql ${PSQL_ARGS} -c "SELECT COUNT(*) FROM pirogue_test.vw_animal_cat;")
EXPECTED=0
if [[ ${RESULT} =~ "${EXPECTED}" ]]; then echo "ok"; else echo "*** ERROR expected result: ${EXPECTED} got ${RESULT}" && ERROR=1; fi

echo "test insert without pkey value (getting default from parent table)"
psql --quiet -v ON_ERROR_STOP="on" -c "INSERT into pirogue_test.vw_animal_cat (fk_breed, eye_color, name, year) VALUES ('1', 'yellow', 'ninja', 1934);"
RESULT=$(psql ${PSQL_ARGS} -c "SELECT cid FROM pirogue_test.vw_animal_cat")
EXPECTED=1101
if [[ ${RESULT} =~ "${EXPECTED}" ]]; then echo "ok"; else echo "*** ERROR expected result: ${EXPECTED} got ${RESULT}" && ERROR=1; fi

echo "test on table with same pkey/ref name"
psql --quiet -v ON_ERROR_STOP="on" -c "INSERT into pirogue_test.vw_aardvark (father) VALUES ('Dark Vador');"
RESULT=$(psql ${PSQL_ARGS} -c "SELECT father FROM pirogue_test.vw_aardvark")
EXPECTED="Dark Vador"
if [[ ${RESULT} =~ "${EXPECTED}" ]]; then echo "ok"; else echo "*** ERROR expected result: ${EXPECTED} got ${RESULT}" && ERROR=1; fi
psql --quiet -v ON_ERROR_STOP="on" -c "UPDATE pirogue_test.vw_aardvark SET father = 'Luke' WHERE father = 'Dark Vador';"
RESULT=$(psql ${PSQL_ARGS} -c "SELECT father FROM pirogue_test.vw_aardvark")
EXPECTED=Luke
if [[ ${RESULT} =~ "${EXPECTED}" ]]; then echo "ok"; else echo "*** ERROR expected result: ${EXPECTED} got ${RESULT}" && ERROR=1; fi

echo "test on table with serial key"
psql --quiet -v ON_ERROR_STOP="on" -c "INSERT into pirogue_test.vw_animal_eagle (weight) VALUES (1.2);"
RESULT=$(psql ${PSQL_ARGS} -c "SELECT weight FROM pirogue_test.vw_animal_eagle")
EXPECTED="1.2"
if [[ ${RESULT} =~ "${EXPECTED}" ]]; then echo "ok"; else echo "*** ERROR expected result: ${EXPECTED} got ${RESULT}" && ERROR=1; fi
psql --quiet -v ON_ERROR_STOP="on" -c "UPDATE pirogue_test.vw_animal_eagle SET weight = 0 WHERE weight = 1.2;"
RESULT=$(psql ${PSQL_ARGS} -c "SELECT weight FROM pirogue_test.vw_animal_eagle")
EXPECTED=0
if [[ ${RESULT} =~ "${EXPECTED}" ]]; then echo "ok"; else echo "*** ERROR expected result: ${EXPECTED} got ${RESULT}" && ERROR=1; fi


echo "exit with $ERROR"
exit $ERROR
