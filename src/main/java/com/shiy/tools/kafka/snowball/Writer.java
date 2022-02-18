package com.shiy.tools.kafka.snowball;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class Writer implements Runnable{

    private final String driverName;
    private final String dbURL;
    private final String username;
    private final String password;
    private final String table;


    private Connection insertConnection;

    private PreparedStatement statement;
    private List<UrlClick> cache;

    private String query ; // PreparedStatement
    private String update; // Statement
    private String delete; // Statement
    private String insert; // Statement

    private long inputCnt = 0L;
    private long updateCnt = 0L;
    private long deleteCnt = 0L;
    private long insertCnt = 0L;

    public Writer(String driverName, String dbURL, String username, String password, String table) {
        this.driverName = driverName;
        this.dbURL = dbURL;
        this.username = username;
        this.password = password;
        this.table = table;
        String[] fieldNames = new String[]{
                "userId",
                "username",
                "url",
                "clickTime",
                "rank",
                "uuid",
                "dateStr",
                "timeStr"};
        this.query = buildQuery(table, fieldNames);
        this.insert = buildInsert(table, fieldNames);
        this.update = buildUpdate(table);
        this.delete = buildDelete(table);
    }

    @Override
    public void run() {
        System.err.println(new Date(System.currentTimeMillis()));
        DateGenerator dateGenerator = new DateGenerator();
        try {
            Class.forName(driverName);
            establishConnection();
            statement = insertConnection.prepareStatement(query);
            this.cache = new ArrayList<>();
            long count = 0;
            while (true) {
                UrlClick urlClick = dateGenerator.gen();
                cache.add(urlClick);
                count++;
                if (cache.size() == 200000) {
                    long startTime = System.nanoTime();
                    flush();
                    long endTime = System.nanoTime();
                    log.info("-------------------------------------------------------------");
                    log.info("write information:");
                    log.info("generate data count:{}", count);
                    log.info("inputCnt:{}", inputCnt);
                    log.info("insertCnt:{}", insertCnt);
                    log.info("updateCnt:{}", updateCnt);
                    log.info("deleteCnt:{}", deleteCnt);
                    log.info("flush use time {}", endTime - startTime);
                    log.info("-------------------------------------------------------------");
                    log.info("");
                }
                if (count == 10000000) {
                    break;
                }
            }
            log.info("close jdbc connection.");
            closeDbConnection();
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.err.println(new Date(System.currentTimeMillis()));
    }

    void flush() {
        try {

            if (cache.isEmpty()) {
                return;
            }

            if (!insertConnection.isValid(5)) {
                log.warn("DB connection timeout.");
                establishConnection();
                statement = insertConnection.prepareStatement(query);
            }
            List<String> upsertSqls = new ArrayList<>();
            for (UrlClick urlClick : cache) {
                String action = urlClick.getAction();
                int userId = urlClick.getUserId();
                String username = urlClick.getUsername();
                String url = urlClick.getUrl();
                Timestamp clickTime = urlClick.getClickTime();
                int rank = urlClick.getRank();
                String uuid = urlClick.getUuid();
                String dateStr = urlClick.getDateStr();
                String timeStr = urlClick.getTimeStr();

                if ("U".equals(action)) {
                    StringBuilder updateBuilder = new StringBuilder();
//                    updateBuilder.append("userId=" + userId + ",");
//                    updateBuilder.append("username='" + username + "',");
                    updateBuilder.append("url='" + url + "',");
                    updateBuilder.append("clickTime='" + clickTime.toString().substring(0, 19) + "',");
                    updateBuilder.append("rank=" + rank + ",");
                    updateBuilder.append("uuid='" + uuid + "',");
                    updateBuilder.append("dateStr='" + dateStr + "',");
                    updateBuilder.append("timeStr='" + timeStr+ "'");

                    String updateConditions = "username='" + username + "'";
                    String updateSql = String.format(update, updateBuilder.toString(), updateConditions);
                    log.debug("update sql is {}", updateSql);
                    updateCnt++;
                    upsertSqls.add(updateSql);
                } else if ("D".equals(action)) {

                    String deleteConditions = "username='" + username + "'";
                    String deleteSql = String.format(delete, deleteConditions);
                    log.debug("delete sql is {}", deleteSql);
                    deleteCnt++;
                    upsertSqls.add(deleteSql);
                } else if ("I".equals(action)) {
                    long startTime = System.currentTimeMillis();
                    statement.setInt(1, userId);
                    statement.setString(2, username);
                    statement.setString(3, url);
                    statement.setTimestamp(4, clickTime);
                    statement.setInt(5, rank);
                    statement.setString(6, uuid);
                    statement.setString(7, dateStr);
                    statement.setString(8, timeStr);
                    statement.addBatch();
                    long endTime = System.currentTimeMillis();
                    if (endTime - startTime > 1) {
                        System.err.println(endTime - startTime);
                    }
                    insertCnt++;
                } else {
                    log.error("Not support action {}. row is {}", action, urlClick);
                }
            }
            inputCnt+=(cache.size());
            int[] batch = statement.executeBatch();
            for (String upsertSql : upsertSqls) {
                statement.execute(upsertSql);
            }
            int affectedRow = Arrays.stream(batch).sum();
            log.debug("JDBC batch flush, insert action affected rows is {}", affectedRow);
            cache.clear();
            upsertSqls.clear();
        } catch (SQLException e) {
            throw new RuntimeException("Execution of JDBC statement failed.", e);
        }
    }

    private void establishConnection() throws SQLException {
        if (username == null) {
            insertConnection = DriverManager.getConnection(dbURL);
        } else {
            insertConnection = DriverManager.getConnection(dbURL, username, password);
        }
    }

    private void closeDbConnection() throws IOException {
        if (insertConnection != null) {
            try {
                insertConnection.close();
            } catch (SQLException se) {
                log.warn("JDBC connection could not be closed: " + se.getMessage());
            } finally {
                insertConnection = null;
            }
        }
    }

    private String buildQuery(String table, String[] fields) {
        String[] marks = new String[fields.length];
        Arrays.fill(marks, "?");
        return "insert into " + table + " (" + StringUtils.join(fields, ",") +
                ") values (" + StringUtils.join(marks, ",") + ")";
    }

    private String buildUpdate(String table) {
        String update = String.format("ALTER TABLE %s UPDATE %s WHERE %s", table, "%s", "%s");
        return update;
    }

    private String buildDelete(String table) {
        String delete = String.format("ALTER TABLE %s DELETE WHERE %s", table, "%s");
        return delete;
    }

    private String buildInsert(String table, String[] fields) {
        String delete = String.format("INSERT INTO %s (%s) values (%s)", table, StringUtils.join(fields, ","), "%s");
        return delete;
    }

}
