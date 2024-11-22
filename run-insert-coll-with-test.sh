#!/bin/sh

java -cp target/DataApiInsertManyTest-*.jar cmd.InsertManyCollectionWrapper -t "$TOKEN_TEST" -d "$DB_TEST" -e TEST $*
