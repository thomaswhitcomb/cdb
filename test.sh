#!/bin/sh

erl -pz ebin -pz ../eunit/ebin/ -s cdb test -s init stop

rm -Rf test.cdb
rm -Rf from_dict_test.cdb
rm -Rf full.cdb

