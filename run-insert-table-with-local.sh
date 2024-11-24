#!/bin/sh

java -cp target/DataApiInsertManyTest-*.jar cmd.InsertManyTableWrapper  -e LOCAL $*
