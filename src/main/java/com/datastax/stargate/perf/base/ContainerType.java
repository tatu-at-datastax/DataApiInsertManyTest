package com.datastax.stargate.perf.base;

public enum ContainerType {
    COLLECTION("Collection"),
    API_TABLE("Table"),
    CQL_TABLE("Table");
    ;

    private final String desc;

    private ContainerType(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return desc;
    }

    public String desc(String name) {
        return String.format("%s '%s'", desc, name);
    }
}
