#!/bin/sh

java -cp target/DataApiInsertManyTest-*.jar cmd.InsertManyCollectionWrapper  -e LOCAL $*
