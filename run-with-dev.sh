#!/bin/sh

java -jar target/DataApiInsertManyTest-0.5-SNAPSHOT.jar -t "$TOKEN_DEV" -d "$DB_DEV" -e DEV $*
