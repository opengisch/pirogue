#!/usr/bin/env bash

set -e
echo "building docs"

pushd docs
sed -i "s/__VERSION__/${TRAVIS_TAG}/" conf.py
make html
popd