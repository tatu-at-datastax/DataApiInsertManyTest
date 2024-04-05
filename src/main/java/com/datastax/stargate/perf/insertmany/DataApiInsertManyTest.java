package com.datastax.stargate.perf.insertmany;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import picocli.CommandLine;
import picocli.CommandLine.Option;

import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.DataAPIOptions;
import com.datastax.astra.client.Database;
import com.dtsx.astra.sdk.db.exception.DatabaseNotFoundException;

public class DataApiInsertManyTest implements Callable<Integer>
{
	enum DataApiEnv {
		PROD(DataAPIOptions.DataAPIDestination.ASTRA),
		DEV(DataAPIOptions.DataAPIDestination.ASTRA_DEV),
		TEST(DataAPIOptions.DataAPIDestination.ASTRA_TEST),
		LOCAL(DataAPIOptions.DataAPIDestination.DSE)
		;
		
		private final DataAPIOptions.DataAPIDestination destination;

		private DataApiEnv(DataAPIOptions.DataAPIDestination d) {
			destination = d;
		}

		public DataAPIOptions.DataAPIDestination destination() {
			return destination;
		}
	}
	
	private final static String TOKEN_PREFIX = "AstraCS:";

	@Option(names = {"-t", "--token"}, required=true,
			description = "Astra Token (starts with 'AstraCS:')")
	private String astraToken;

	@Option(names = {"-i", "--db-id"}, required=true,
			description = "Database ID (UUID)")
	private String dbIdAsString;

	@Option(names = {"-e", "--env"}, required=false,
			description = "Astra env (PROD [default], DEV, TEST, LOCAL)")
	private DataApiEnv env = DataApiEnv.PROD;

	@Option(names = {"-n", "--namespace"}, required=false,
			description = "Namespace (like 'ks')")
	private String namespace = null;

	
	@Override
	public Integer call() throws Exception {
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
			if (namespace == null || namespace.isEmpty()) {
				db = client.getDatabase(dbId);
			} else {
				db = client.getDatabase(dbId, namespace);
			}
		} catch (DatabaseNotFoundException dbNfe) {
			System.err.printf(" FAIL: (%s) %s\n", dbNfe.getClass().getSimpleName(),
					dbNfe.getMessage());
			return 3;
		}
		System.out.printf(" connected: namespace '%s'\n", db.getNamespaceName());

		System.out.printf("Fetch Collections in the Database: ");

		Stream<String> collectionNames;

		try {
			collectionNames	= db.listCollectionNames();
			System.out.println(collectionNames.toList());
		} catch (Exception e) {
			System.err.printf(" FAIL: (%s) %s\n", e.getClass().getSimpleName(),
					e.getMessage());
			return 3;
		}

		System.out.println("DONE!");
		
		return 0;
	}
	
	public static void main(String[] args)
    {
		int exitCode = new CommandLine(new DataApiInsertManyTest()).execute(args);
        System.exit(exitCode);
	}
}
