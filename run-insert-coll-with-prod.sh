#!/bin/sh

java -cp target/DataApiInsertManyTest-*.jar cmd.InsertManyCollectionWrapper -t "$TOKEN_PROD" -d "$DB_PROD" -e PROD $*
