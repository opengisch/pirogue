#!/usr/bin/env

# build docs
pushd docs
make html

mkdir publish && pushd publish

git config --global user.email "qgisninja@gmail.com"
git config --global user.name "Geo Ninja"
git clone https://${GH_TOKEN}@github.com/opengis.ch/pirogue.git --depth 1 --branch gh-pages

cp -R ../_build/html/* .

git add .
git commit -m "API docs for: $TRAVIS_TAG"

git push -v

popd
popd