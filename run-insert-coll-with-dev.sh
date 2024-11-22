#!/bin/sh

java -cp target/DataApiInsertManyTest-*.jar cmd.InsertManyCollectionWrapper -t "$TOKEN_DEV" -d "$DB_DEV" -e DEV $*
