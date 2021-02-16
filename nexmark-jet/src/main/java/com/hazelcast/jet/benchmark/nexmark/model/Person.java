package com.hazelcast.jet.benchmark.nexmark.model;

public class Person extends Event {
    private final String name;
    private final String state;

    public Person(long id, long timestamp, String name, String state) {
        super(id, timestamp);
        this.name = name;
        this.state = state;
    }

    public String name() {
        return name;
    }

    public String state() {
        return state;
    }
}
