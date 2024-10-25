package com.datastax.stargate.perf.insertmany;

import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.DataAPIOptions;
import com.datastax.astra.client.Database;
import com.datastax.astra.client.admin.DatabaseAdmin;
import com.datastax.astra.internal.auth.UsernamePasswordTokenProvider;
import com.dtsx.astra.sdk.db.exception.DatabaseNotFoundException;
import picocli.CommandLine;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for various tests
 */
abstract class DataApiTestBase {
    enum DataApiEnv {
        PROD(DataAPIOptions.DataAPIDestination.ASTRA),
        DEV(DataAPIOptions.DataAPIDestination.ASTRA_DEV),
        TEST(DataAPIOptions.DataAPIDestination.ASTRA_TEST),
        LOCAL(DataAPIOptions.DataAPIDestination.DSE)
        ;

        private final DataAPIOptions.DataAPIDestination destination;

        DataApiEnv(DataAPIOptions.DataAPIDestination d) {
            destination = d;
        }

        public DataAPIOptions.DataAPIDestination destination() {
            return destination;
        }
    }

    final static String TOKEN_PREFIX = "AstraCS:";

    @CommandLine.Option(names = {"-t", "--token"},
            description = "Astra Token (starts with 'AstraCS:' if used)")
    String astraToken = "";

    @CommandLine.Option(names = {"-d", "--db", "--db-id"},
            description = "Database ID (UUID)")
    String dbIdAsString = "";

    @CommandLine.Option(names = {"-e", "--env"},
            description = "Astra env (PROD [default], DEV, TEST, LOCAL)")
    DataApiEnv env = DataApiEnv.PROD;

    @CommandLine.Option(names = {"-n", "--namespace"},
            description = "Namespace (default 'default_keyspace')")
    String ns = "default_keyspace";

    @CommandLine.Option(names = {"-c", "--collection-name"},
            defaultValue = "insert_many_test",
            description = "Collection name (default: 'insert_many_test')")
    String collectionName;

    @CommandLine.Option(names = {"-v", "--vector-length"},
            description = "Vector size; 0 to disable (default: 1536)")
    int vectorLength = 1536;

    @CommandLine.Option(names = {"-x", "--index", "--add-indexes"}, arity="1",
            description = "Whether to Index (all) Fields or not (default: true)")
    boolean addIndexes = true;

    @CommandLine.Option(names = {"-o", "--ordered", "--ordered-inserts"}, arity="1",
            description = "Whether inserts are Ordered (default: false)")
    boolean orderedInserts = false;

    @CommandLine.Option(names = "--skip-init", arity="0",
            description = "Skip initialization (use existing collection)")
    boolean skipInit = false;

    protected Database initializeDB(AtomicInteger exitCode)
    {
        Database db;

        // Astra differs from local:
        if (env != DataApiEnv.LOCAL) {
            // Some validation only matters for Astra (non-local)
            if (!astraToken.startsWith(TOKEN_PREFIX)
                    && env != DataApiEnv.LOCAL) {
                System.err.printf("Token does not start with prefix (has to, in %s) '%s': %s\n",
                        env, TOKEN_PREFIX, astraToken);
                exitCode.set(1);
                return null;
            }
            final UUID dbId;
            try {
                dbId = UUID.fromString(dbIdAsString);
            } catch (Exception e) {
                System.err.printf("Database id not valid UUID: %s\n", dbIdAsString);
                exitCode.set(2);
                return null;
            }
            final DataAPIClient client = createClient(astraToken);
            System.out.printf("Connecting to database '%s' (env '%s')...",
                    dbId, env.name());

            try {
                if (ns == null || ns.isEmpty()) {
                    db = client.getDatabase(dbId);
                } else {
                    db = client.getDatabase(dbId, ns);
                }
            } catch (DatabaseNotFoundException dbNfe) {
                System.err.printf("\n  FAIL: (%s) %s\n", dbNfe.getClass().getSimpleName(),
                        dbNfe.getMessage());
                exitCode.set(3);
                return null;
            }
            System.out.printf(" connected: namespace '%s'\n", db.getNamespaceName());
        } else { // LOCAL env
            String token = new UsernamePasswordTokenProvider("cassandra", "cassandra").getToken();
            final DataAPIClient client = createClient(token);
            System.out.printf("Connecting to LOCAL database...");
            db = client.getDatabase("http://localhost:8181", "default_keyspace");
            System.out.printf(" connected: namespace '%s'\n", db.getNamespaceName());
        }

        System.out.printf("Check existence of namespace '%s'...", db.getNamespaceName());
        DatabaseAdmin admin = db.getDatabaseAdmin();
        if (admin.namespaceExists(db.getNamespaceName())) {
            System.out.println("namespace exists.");
        } else {
            System.out.printf("namespace does not exist: will try create... ");
            admin.createNamespace(db.getNamespaceName());
            System.out.println("Created!");
        }

        return db;
    }

    private DataAPIClient createClient(String token) {
        System.out.print("Creating DataAPIClient...");
        DataAPIOptions.DataAPIClientOptionsBuilder optBuilder = DataAPIOptions.builder()
                .withDestination(env.destination());
        DataAPIClient client = new DataAPIClient(token, dataApiOptions(optBuilder).build());
        System.out.println(" created.");
        return client;
    }

    protected DataAPIOptions.DataAPIClientOptionsBuilder dataApiOptions(
            DataAPIOptions.DataAPIClientOptionsBuilder builder) {
        return builder;
    }
}
