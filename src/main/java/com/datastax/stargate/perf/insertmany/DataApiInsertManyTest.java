package com.datastax.stargate.perf.insertmany;

import java.util.UUID;
import java.util.concurrent.Callable;

import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.Database;

import picocli.CommandLine;
import picocli.CommandLine.Option;

public class DataApiInsertManyTest implements Callable<Integer>
{
	private final static String TOKEN_PREFIX = "AstraCS:";

	@Option(names = {"-t", "--token"}, required=true,
			description = "Astra Token (starts with 'AstraCS:')")
	private String astraToken;

	@Option(names = {"-i", "--db-id"}, required=true,
			description = "Database Id (UUID)")
	private String dbIdAsString;

	@Option(names = {"-r", "--db-region"}, required=false,
			description = "Database region (like 'us-east1')")
	private String dbRegion = "us-east1";

	@Option(names = {"-k", "--keyspace"}, required=false,
			description = "Keyspace (like 'ks')")
	private String keyspace = "default";

	
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
		DataAPIClient client = new DataAPIClient(astraToken);
		client.getDatabase(astraToken);
		System.out.println(" created");

		System.out.printf("Connecting to database '%s'...", dbId);
		Database db = client.getDatabase(dbId);
		System.out.printf(" connected: namespace '%s'\n", db.getNamespaceName());

		return 0;
	}
	
	public static void main(String[] args)
    {
		int exitCode = new CommandLine(new DataApiInsertManyTest()).execute(args);
        System.exit(exitCode);
	}
}
