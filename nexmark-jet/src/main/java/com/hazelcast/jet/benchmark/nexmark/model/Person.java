package com.hazelcast.jet.benchmark.nexmark.model;

public class Person extends Event {
    private final String name;

    public Person(long id, long timestamp, String name) {
        super(id, timestamp);
        this.name = name;
    }

    public String name() {
        return name;
    }
}
