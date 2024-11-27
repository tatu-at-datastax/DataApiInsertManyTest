package com.datastax.stargate.perf.base;

import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.DataAPIDestination;
import com.datastax.astra.client.admin.DatabaseAdmin;
import com.datastax.astra.client.core.auth.UsernamePasswordTokenProvider;
import com.datastax.astra.client.core.options.DataAPIClientOptions;
import com.datastax.astra.client.core.options.TimeoutOptions;
import com.datastax.astra.client.databases.Database;
import com.dtsx.astra.sdk.db.exception.DatabaseNotFoundException;
import picocli.CommandLine;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for various tests
 */
public abstract class DataApiTestBase {
    enum DataApiEnv {
        PROD(DataAPIDestination.ASTRA),
        DEV(DataAPIDestination.ASTRA_DEV),
        TEST(DataAPIDestination.ASTRA_TEST),
        LOCAL(DataAPIDestination.DSE)
        ;

        private final DataAPIDestination destination;

        DataApiEnv(DataAPIDestination d) {
            destination = d;
        }

        public DataAPIDestination destination() {
            return destination;
        }
    }

    final static String TOKEN_PREFIX = "AstraCS:";

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
            description = "Skip initialization (use existing collection)")
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
            System.out.printf(" connected: keyspace '%s'\n", db.getKeyspaceName());
        } else { // LOCAL env
            String token = new UsernamePasswordTokenProvider("cassandra", "cassandra").getToken();
            final DataAPIClient client = createClient(token);
            System.out.print("Connecting to LOCAL database...");
            db = client.getDatabase("http://localhost:8181", "default_keyspace");
            System.out.printf(" connected: keyspace '%s'\n", db.getKeyspaceName());
        }

        System.out.printf("Check existence of keyspace '%s'...", db.getKeyspaceName());
        DatabaseAdmin admin = db.getDatabaseAdmin();
        System.out.println(" (DatabaseAdmin created) ");
        if (admin.keyspaceExists(db.getKeyspaceName())) {
            System.out.println("keyspace exists.");
        } else {
            System.out.print("keyspace does not exist: will try create... ");
            admin.createKeyspace(db.getKeyspaceName());
            System.out.println("Created!");
        }

        return db;
    }

    protected  DataAPIClient createClient(String token) {
        System.out.print("Creating DataAPIClient...");
        DataAPIClientOptions.DataAPIClientOptionsBuilder optBuilder = DataAPIClientOptions.builder()
                .withDestination(env.destination());
        // Retry defaults would be 3/100 msec; change to 3/50 msec
        optBuilder = optBuilder.withHttpRetries(3, Duration.ofMillis(50L));
        // Also timeout settings: increase slightly from defaults:
        optBuilder = optBuilder.withTimeoutOptions(new TimeoutOptions()
                        .connectTimeoutMillis(15_000L)
                        .requestTimeoutMillis(20_000L));
        DataAPIClient client = new DataAPIClient(token, dataApiOptions(optBuilder).build());
        System.out.println(" created.");
        return client;
    }

    protected abstract DataAPIClientOptions.DataAPIClientOptionsBuilder dataApiOptions(
            DataAPIClientOptions.DataAPIClientOptionsBuilder builder);
}
