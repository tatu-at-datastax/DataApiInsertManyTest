package com.datastax.stargate.perf.base;

import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.DataAPIDestination;
import com.datastax.astra.client.admin.DatabaseAdmin;
import com.datastax.astra.client.core.auth.UsernamePasswordTokenProvider;
import com.datastax.astra.client.core.http.HttpClientOptions;
import com.datastax.astra.client.core.options.DataAPIClientOptions;
import com.datastax.astra.client.core.options.TimeoutOptions;
import com.datastax.astra.client.databases.Database;
import com.datastax.astra.client.databases.DatabaseOptions;
import com.dtsx.astra.sdk.db.exception.DatabaseNotFoundException;
import picocli.CommandLine;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for various tests
 */
public abstract class DataApiTestBase
    implements Callable<Integer>
{
    enum DataApiEnv {
        PROD(DataAPIDestination.ASTRA),
        DEV(DataAPIDestination.ASTRA_DEV),
        TEST(DataAPIDestination.ASTRA_TEST),
        // Hmmmh. Should we allow DSE or CASSANDRA here?
        LOCAL(DataAPIDestination.HCD)
        ;

        private final DataAPIDestination destination;

        DataApiEnv(DataAPIDestination d) {
            destination = d;
        }

        public DataAPIDestination destination() {
            return destination;
        }
    }

    private final static String TOKEN_PREFIX = "AstraCS:";

    // // // Immutable configuration (not from command line)

    protected final ContainerType containerType;

    // // // Connection settings, initialization

    @CommandLine.Option(names = {"-t", "--token"},
            description = "Astra Token (starts with 'AstraCS:' if used)")
    protected String astraToken = "";

    @CommandLine.Option(names = {"-d", "--db", "--db-id"},
            description = "Database ID (UUID)")
    protected String dbIdAsString = "";

    @CommandLine.Option(names = {"-e", "--env"},
            description = "Astra env (PROD [default], DEV, TEST, LOCAL)")
    protected DataApiEnv env = DataApiEnv.PROD;

    @CommandLine.Option(names = {"-k", "--keyspace", "--namespace"},
            description = "Keyspace (default 'default_keyspace')")
    protected String ns = "default_keyspace";

    @CommandLine.Option(names = "--skip-init", arity="0",
            description = "Skip initialization (use existing container)")
    protected boolean skipInit = false;

    // // // Operational settings (concurrency, rate limiting, batch size)

    @CommandLine.Option(names = {"-a", "--agent-count"},
            description = "Agent count (also: thread count) (default: 10)")
    protected int agentCount = 10;

    @CommandLine.Option(names = {"-b", "--batch-size"},
            description = "Batch size for inserts (default: 20)")
    protected int batchSize = 20;

    @CommandLine.Option(names = {"-o", "--ordered", "--ordered-inserts"}, arity="1",
            description = "Whether inserts are Ordered (default: false)")
    protected boolean orderedInserts = false;

    @CommandLine.Option(names = {"-r", "--rate", "--rate-limit"},
            description = "Rate limit as RPS (default: 100)")
    protected int rateLimitRPS = 100;

    // // // Content limits/settings

    // Maximum allowed wrt Base64-encoded Blog  -> 8000 bytes
    @CommandLine.Option(names = {"-v", "--vector-length"},
            description = "Vector size; 0 to disable (default: 1500)")
    protected int vectorLength = 1500;

    protected DataApiTestBase(ContainerType containerType) {
        this.containerType = containerType;
    }

    // Skeletal implementation of the call() method suitable for different test types
    // (insertMany etc) and backends (collection, API table, CQL table)
    @Override
    public final Integer call()
    {
        final AtomicInteger exitCodeWrapper = new AtomicInteger(-1);
        Database db = initializeDB(exitCodeWrapper);

        if (db == null) {
            return exitCodeWrapper.get();
        }

        // Check out existing Collections/Tables:
        System.out.printf("Fetch names of existing %ss in the database: ", containerType.toString());
        List<String> containerNames;

        try {
            containerNames = switch (containerType) {
                case COLLECTION -> db.listCollectionNames();
                // We use Data API access for both API Table and CQL Table for now
                case API_TABLE, CQL_TABLE -> db.listTableNames();
            };
            System.out.println(containerNames);
        } catch (Exception e) {
            System.err.printf("\n  FAIL/base1: (%s) %s\n", e.getClass().getSimpleName(),
                    e);
            return 1;
        }

        // Create the test client
        DataApiTestClient testClient;
        try {
            testClient = createTestClient(db);
        } catch (Exception e) {
            System.err.printf("\n  FAIL/base2: (%s) %s\n", e.getClass().getSimpleName(),
                    e);
            return 2;
        }

        // Initialize...
        System.out.printf("Initialize test client (%s; skipInit? %s):\n",
                containerDesc(), skipInit);
        try {
            testClient.initialize(skipInit, createIndexes());
        } catch (Exception e) {
            System.err.printf("\n  FAIL/base3: (%s) %s\n", e.getClass().getName(),
                    e);
            return 3;
        }
        System.out.printf("Ok: Initialization of %s successful.\n", containerDesc());

        // Validate functioning of test operations
        System.out.printf("Validate that test operations on %s work.\n", containerDesc());
        try {
            testClient.validate();
        } catch (Exception e) {
            System.err.printf("\n  FAIL/base4: (%s) %s\n", e.getClass().getSimpleName(),
                    e);
            return 4;
        }
        System.out.printf("Ok: Validation of %s successful.\n", containerDesc());

        // And then warm-up, run test
        System.out.printf("Start warm-up, run test against %s.\n", containerDesc());
        try {
            testClient.runWarmupAndTest(agentCount, rateLimitRPS);
        } catch (Exception e) {
            System.err.printf("\n  FAIL/base5: (%s) %s\n", e.getClass().getName(),
                    e);
            return 5;
        }

        System.out.println();
        System.out.println("DONE!");
        return 0;
    }

    protected abstract String containerName();
    protected String containerDesc() {
        return containerType.desc(containerName());
    }

    protected abstract boolean createIndexes();

    protected abstract DataApiTestClient createTestClient(Database db) throws Exception;

    protected Database initializeDB(AtomicInteger exitCode)
    {
        Database db;

        // Astra differs from local:
        if (env != DataApiEnv.LOCAL) {
            // Some validation only matters for Astra (non-local)
            if (!astraToken.startsWith(TOKEN_PREFIX)) {
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
                DatabaseOptions dbOpts = new DatabaseOptions()
                        .token(astraToken)
                        .dataAPIClientOptions(new DataAPIClientOptions()
                                .destination(env.destination()));

                if (ns != null && !ns.isEmpty()) {
                    dbOpts = dbOpts.keyspace(ns);
                }
                db = client.getDatabase(dbId, dbOpts);

            } catch (DatabaseNotFoundException dbNfe) {
                System.err.printf("\n  FAIL/b0: (%s) %s\n", dbNfe.getClass().getSimpleName(),
                        dbNfe.getMessage());
                exitCode.set(3);
                return null;
            }
            System.out.printf(" connected: keyspace '%s'\n", db.getKeyspace());
        } else { // LOCAL env
            String token = new UsernamePasswordTokenProvider("cassandra", "cassandra").getToken();
            final DataAPIClient client = createClient(token);
            System.out.print("Connecting to LOCAL database...");
            db = client.getDatabase("http://localhost:8181");
                new DatabaseOptions().keyspace("default_keyspace")
                                .dataAPIClientOptions(new DataAPIClientOptions()
                                        .destination(env.destination()));
            System.out.printf(" connected: keyspace '%s'\n", db.getKeyspace());
        }

        System.out.printf("Check existence of keyspace '%s'...", db.getKeyspace());
        DatabaseAdmin admin = db.getDatabaseAdmin();
        System.out.println(" (DatabaseAdmin created) ");
        if (admin.keyspaceExists(db.getKeyspace())) {
            System.out.println("keyspace exists.");
        } else {
            System.out.print("keyspace does not exist: will try create... ");
            admin.createKeyspace(db.getKeyspace());
            System.out.println("Created!");
        }

        return db;
    }

    protected  DataAPIClient createClient(String token) {
        System.out.print("Creating DataAPIClient...");
        DataAPIClientOptions opts = new DataAPIClientOptions()
                .destination(env.destination());
        // Retry defaults would be 3/100 msec; change to 3/50 msec
        opts = opts.httpClientOptions(new HttpClientOptions()
                .httpRetries(3, Duration.ofMillis(50L)));
        // Also timeout settings: increase slightly from defaults:
        opts = opts.timeoutOptions(new TimeoutOptions()
                        .connectTimeoutMillis(15_000L)
                        .requestTimeoutMillis(20_000L));
        DataAPIClient client = new DataAPIClient(token, dataApiOptions(opts));
        System.out.println(" created.");
        return client;
    }

    protected abstract DataAPIClientOptions dataApiOptions(
            DataAPIClientOptions builder);
}
