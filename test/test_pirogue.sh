#! /usr/bin/env bash

set -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export PYTHONPATH=${DIR}/..:${PYTHONPATH}

PGSERVICE=pirogue_test ${DIR}/../scripts/pirogue join pirogue_test.animal pirogue_test.cat