package com.inforefiner.tools.kafka;

import java.io.Closeable;
import java.io.IOException;

public class Student implements Closeable {

    private final String name;

    public Student(String name) {
        this.name = name;
    }

    public void test(){
        System.err.println(name);
    }

    @Override
    public void close() throws IOException {
        System.err.println("close......" + name);
    }
}
