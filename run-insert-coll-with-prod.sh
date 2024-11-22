#!/bin/sh

java -jar target/DataApiInsertManyTest-*.jar -t "$TOKEN_PROD" -d "$DB_PROD" -e PROD $*
