#!/bin/sh

java -cp target/DataApiInsertManyTest-*.jar cmd.InsertManyTableWrapper -t "$TOKEN_TEST" -d "$DB_TEST" -e TEST $*
