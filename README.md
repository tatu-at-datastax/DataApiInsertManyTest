# DataApiInsertManyTest

Test suite for analysing performance of "InsertMany" operation of DataStax Data API.

## Usage

Tests are typically run by executing one of following scripts:

```bash
./run-with-prod.sh
./run-with-test.sh
./run-with-dev.sh
```

which are simple wrappers around executing built jar with appropriate default arguments, but also allowing
custom argument overrides.
Looking at first script we see:

```bash
java -jar target/DataApiInsertManyTest-*.jar -t "$TOKEN_PROD" -d "$DB_PROD" -e PROD $*
```

so it requires 2 environment variables to be set: to do that you will want to copy `setup-keys.sh.tmpl`
into `setup-keys.sh`; add your Astra token(s) and database id(s) to it, and then source it:

```bash
source setup-keys.sh
```

In addition to setting up these environment variables, you will need to build the test jar:

```bash
./mvnw clean install
```

and after doing this, you can run the test suite:

```bash
./run-with-prod.sh 
# Or with additional arguments:
./run-with-prod.sh --skip-init -c my_test_collection
```
