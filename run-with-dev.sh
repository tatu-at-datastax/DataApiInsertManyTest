#!/bin/sh

java -jar target/DataApiInsertManyTest-*.jar -t "$TOKEN_DEV" -d "$DB_DEV" -e DEV $*
