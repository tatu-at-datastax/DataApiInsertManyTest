#!/bin/sh

java -jar target/DataApiInsertManyTest-0.5-SNAPSHOT.jar -t "$TOKEN_TEST" -d "$DB_TEST" -e TEST $*
