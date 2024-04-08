#!/bin/sh

java -jar target/DataApiInsertManyTest-0.5-SNAPSHOT.jar -t "$TOKEN_PROD" -d "$DB_PROD" -e PROD $*
