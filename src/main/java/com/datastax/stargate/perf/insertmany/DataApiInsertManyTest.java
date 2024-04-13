package com.datastax.stargate.perf.insertmany;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import com.datastax.stargate.perf.insertmany.agent.MetricsCollector;
import com.datastax.stargate.perf.insertmany.entity.ItemCollection;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.DataAPIOptions;
import com.datastax.astra.client.Database;
import com.dtsx.astra.sdk.db.exception.DatabaseNotFoundException;

@CommandLine.Command(name = "DataApiInsertManyTest", mixinStandardHelpOptions=true)
public class DataApiInsertManyTest implements Callable<Integer>
{
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

    private final static String TOKEN_PREFIX = "AstraCS:";

    @Option(names = {"-t", "--token"}, required=true,
            description = "Astra Token (starts with 'AstraCS:')")
    String astraToken;

    @Option(names = {"-d", "--db", "--db-id"}, required=true,
            description = "Database ID (UUID)")
    String dbIdAsString;

    @Option(names = {"-e", "--env"},
            description = "Astra env (PROD [default], DEV, TEST, LOCAL)")
    DataApiEnv env = DataApiEnv.PROD;

    @Option(names = {"-n", "--namespace"},
            description = "Namespace (like 'default_ns')")
    String ns = null;

    @Option(names = {"-c", "--collection-name"},
            defaultValue = "insert_many_test",
            description = "Collection name (default: 'insert_many_test')")
    String collectionName;

    @Option(names = {"-v", "--vector-length"},
            description = "Vector size; 0 to disable (default: 1536)")
    int vectorLength = 1536;

    @Option(names = {"-x", "--index", "--add-indexes"}, arity="1",
            description = "Whether to Index (all) Fields or not (default: true)")
    boolean addIndexes = true;

    @Option(names = {"-o", "--ordered", "--ordered-inserts"}, arity="1",
            description = "Whether inserts are Ordered (default: false)")
    boolean orderedInserts = false;

    @Option(names = "--skip-init", arity="0",
          description = "Skip initialization (use existing collection)")
    boolean skipInit = false;

    @Option(names = {"-b", "--batch-size"},
            description = "Batch size for inserts (default: 20)")
    int batchSize = 20;

    @Option(names = {"-r", "--rate", "--rate-limit"},
            description = "Rate limit as RPS (default: 100)")
    int rateLimitRPS = 100;

    @Option(names = {"-a", "--agent-count"},
            description = "Agent count (also: thread count) (default: 10)")
    int agentCount = 10;

    @Override
    public Integer call() {
        if (!astraToken.startsWith(TOKEN_PREFIX)) {
            System.err.printf("Token does not start with prefix '%s': %s\n",
                    TOKEN_PREFIX, astraToken);
            return 1;
        }
        UUID dbId;
        try {
            dbId = UUID.fromString(dbIdAsString);
        } catch (Exception e) {
            System.err.printf("Database id not valid UUID: %s\n", dbIdAsString);
            return 2;
        }
        System.out.print("Creating DataAPIClient...");
        DataAPIClient client = new DataAPIClient(astraToken,
                DataAPIOptions.builder()
                .withDestination(env.destination())
                .build());
        System.out.println(" created");

        System.out.printf("Connecting to database '%s' (env '%s')...",
                dbId, env.name());
        Database db;

        try {
            if (ns == null || ns.isEmpty()) {
                db = client.getDatabase(dbId);
            } else {
                db = client.getDatabase(dbId, ns);
            }
        } catch (DatabaseNotFoundException dbNfe) {
            System.err.printf("\n  FAIL: (%s) %s\n", dbNfe.getClass().getSimpleName(),
                    dbNfe.getMessage());
            return 3;
        }
        System.out.printf(" connected: namespace '%s'\n", db.getNamespaceName());

        System.out.printf("Fetch names of existing Collections in the database: ");

        Stream<String> collectionNames;

        try {
            collectionNames	= db.listCollectionNames();
            System.out.println(collectionNames.toList());
        } catch (Exception e) {
            System.err.printf("\n  FAIL: (%s) %s\n", e.getClass().getSimpleName(),
                    e.getMessage());
            return 3;
        }

        InsertManyClient testClient = new InsertManyClient(db, collectionName,
                vectorLength, orderedInserts, batchSize);
        System.out.printf("Initialize test client (collection '%s'):\n", collectionName);
        try {
            testClient.initialize(skipInit, addIndexes);
        } catch (Exception e) {
            System.err.printf("\n  FAIL: (%s) %s\n", e.getClass().getSimpleName(),
                    e.getMessage());
            return 3;
        }
        System.out.printf("Ok: Initialization of '%s' successful.\n", collectionName);
        System.out.printf("Validate that inserts to '%s' work.\n", collectionName);
        try {
            testClient.validate();
        } catch (Exception e) {
            System.err.printf("\n  FAIL: (%s) %s\n", e.getClass().getSimpleName(),
                    e.getMessage());
            return 4;
        }
        System.out.printf("Ok: Validation of '%s' successful.\n", collectionName);

        System.out.printf("Start warm-up, run test against '%s'.\n", collectionName);
        try {
            testClient.runWarmupAndTest(agentCount, rateLimitRPS);
        } catch (Exception e) {
            System.err.printf("\n  FAIL: (%s) %s\n", e.getClass().getSimpleName(),
                    e.getMessage());
            return 5;
        }

        System.out.println();
        System.out.println("DONE!");
        return 0;
    }

    public static void main(String[] args)
    {
        int exitCode = new CommandLine(new DataApiInsertManyTest()).execute(args);
        System.exit(exitCode);
    }
}
