package com.inforefiner.tools.kafka.snowball;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SnowBallJDBCSinkMain {

    private static String driverName = "com.inforefiner.snowball.SnowballDriver";
    private static String dbURL = "jdbc:snowball://192.168.1.89:8123,192.168.1.97:8123,192.168.1.98:8123/lqr";
    private static String username = "default";
    private static String password = "";
    private static String table = "shiy_custom_jdbc_sink_distributed";

    public static void main(String[] args) {
        log.info("starting snowball jdbc test......");
        Writer writer = new Writer(driverName, dbURL, username, password, table);
        Thread thread = new Thread(writer);
        thread.start();
    }


}
