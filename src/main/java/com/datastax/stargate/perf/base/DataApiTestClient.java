package com.datastax.stargate.perf.base;

import com.datastax.astra.client.databases.Database;

import java.util.Objects;

/**
 * Base class for per-operation Clients (like InsertManyClient), called by
 * generic base test class.
 */
public abstract class DataApiTestClient {
    protected final Database db;

    protected final ContainerType containerType;
    protected final String containerName;

    protected DataApiTestClient(Database db,
                                ContainerType containerType, String containerName) {
        this.db = Objects.requireNonNull(db);
        this.containerType = Objects.requireNonNull(containerType);
        this.containerName = Objects.requireNonNull(containerName);
    }

    // // // Life-cycle methods

    public abstract void initialize(boolean skipContainerRecreate,
                                    boolean addIndexes) throws Exception;

    public abstract void validate() throws Exception;

    public abstract void runWarmupAndTest(int threadCount, int testMaxRPS)
        throws Exception;

    // // // Helper methods for subclasses; simple accessors

    public String containerDesc() {
        return containerType.desc(containerName);
    }

    // // // Helper methods for subclasses; container access

    protected boolean containerExists() {
        return switch (containerType) {
            case COLLECTION -> db.collectionExists(containerName);
            case TABLE -> db.tableExists(containerName);
        };
    }

    protected void dropContainer() {
        switch (containerType) {
            case COLLECTION:
                db.dropCollection(containerName);
                break;
            case TABLE:
            default:
                db.dropTable(containerName);
        }
    }
}
