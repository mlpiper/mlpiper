#!/usr/bin/env bash -x

DIR=$1
CLI_WHL=$2
REST_CLIENT_WHL=$3


rm -rf dist/$DIR
mkdir -p dist/$DIR
cp $CLI_WHL dist/$DIR/
cp $REST_CLIENT_WHL dist/$DIR/
cp INSTALL.txt dist/$DIR/

cd dist
tar cvfz $DIR.tgz $DIR
rm -rf $DIR
