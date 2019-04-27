#!/usr/bin/env bash

set -e

# build docs
pushd docs
sed -i "s/__VERSION__/${TRAVIS_TAG}/" conf.py
echo "building docs"
make html

mkdir publish && pushd publish

echo "clone repo at gh-pages branch"
git config --global user.email "qgisninja@gmail.com"
git config --global user.name "Geo Ninja"
git clone https://${GH_TOKEN}@github.com/opengisch/pirogue.git --depth 1 --branch gh-pages

cp -R ../_build/html/* .

echo "git add, commit push"
git add .
git commit -m "API docs for: $TRAVIS_TAG"

git push -v origin HEAD:gh-pages

popd
popd