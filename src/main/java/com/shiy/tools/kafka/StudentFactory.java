package com.shiy.tools.kafka;

@FunctionalInterface
public interface StudentFactory {

    Student create1(String name);

    static StudentFactory getzzz() {
        return Student::new;
    }

}
