#!/bin/sh

java -cp target/DataApiInsertManyTest-*.jar cmd.InsertManyTableWrapper -t "$TOKEN_DEV" -d "$DB_DEV" -e DEV $*
