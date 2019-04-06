#! /usr/bin/env bash

set -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export PYTHONPATH=${DIR}/..:${PYTHONPATH}

${DIR}/../scripts/pirogue join pirogue_test.animal pirogue_test.cat -p pirogue_test