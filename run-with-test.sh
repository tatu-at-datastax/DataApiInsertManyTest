#!/bin/sh

java -jar target/DataApiInsertManyTest-*.jar -t "$TOKEN_TEST" -d "$DB_TEST" -e TEST $*
