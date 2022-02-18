package com.shiy.tools.kafka.generator;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author P0007
 * @version 1.0
 * @date 2020/4/22 16:33
 */
@Slf4j
public class FiniStateUrlGenerator {

    private static final String SEPARATOR = ",";

    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public static SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");

    public static List<String> status = Arrays.asList("START", "MIDDLE_1", "MIDDLE_2", "MIDDLE_3", "FINAL_1", "FINAL_2");



    /*
    * 记录username和final state状态
    * */
    public static Map<String, String> finalStateMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        init();
        for (int i = 0; i < 10000; i++) {
            String record = generateUrlClickDate();
            System.out.println(record);
        }

        for (String username : finalStateMap.keySet()) {
            System.err.println(username + " " + finalStateMap.get(username));
        }

    }


    public static String generateUrlClickDate() {
        Random random = new Random(System.currentTimeMillis());
        int nextInt = random.nextInt(10);
        Integer userId = 65 + nextInt; // userId
        String username = getOrCreateUsername(); // username
        String url = "http://127.0.0.1/api/" + (char) ('H' + random.nextInt(4)); // url
        Timestamp clickTime = new Timestamp(System.currentTimeMillis() - 7171000); // clickTime
        Integer rank = random.nextInt(100); // rank
        String uuid = UUID.randomUUID().toString(); // uuid
        Date date = new Date(clickTime.getTime());
        String dateStr = dateFormat.format(date); // dateStr
        String timeStr = timeFormat.format(date); // timeStr
        String finalState = finalStateMap.get(username); // finalState

        String urlClickDate = randomNullCol(userId.toString(), username, url, clickTime.toString(),
                rank.toString(), uuid, dateStr, timeStr, finalState);

        return urlClickDate;
    }

    private static String randomNullCol(String userId, String username, String url,
                                        String clickTime, String rank, String uuid,
                                        String dateStr, String timeStr, String finalState) {

        Random random = new Random(System.currentTimeMillis());
        StringBuilder stringBuilder = new StringBuilder();

        if (StringUtils.isNoneEmpty(finalState)  && isFinalState(finalState)) {
            int nextInt = random.nextInt(100);
            if (nextInt == 0) {
                finalState = "";
                log.info("set username {} clickTime {} uuid {} final state to null", username, clickTime, uuid);
            }
            stringBuilder // userId
                    .append(SEPARATOR).append(username) //username
                    .append(SEPARATOR) // url
                    .append(SEPARATOR).append(clickTime) // clickTime
                    .append(SEPARATOR) // rank
                    .append(SEPARATOR).append(uuid) // uuid
                    .append(SEPARATOR) // dateStr
                    .append(SEPARATOR) // timeStr
                    .append(SEPARATOR).append(finalState); // finalState
        } else {
            int nextInt = random.nextInt(5);
            switch (nextInt) {
                case 0:
                    stringBuilder.append(userId) // userId
                            .append(SEPARATOR).append(username) //username
                            .append(SEPARATOR).append(url) // url
                            .append(SEPARATOR).append(clickTime) // clickTime
                            .append(SEPARATOR) // rank
                            .append(SEPARATOR).append(uuid) // uuid
                            .append(SEPARATOR).append(dateStr) // dateStr
                            .append(SEPARATOR).append(timeStr) // timeStr
                            .append(SEPARATOR).append(finalState); // finalState
                    break;
                case 1:
                    stringBuilder.append(userId) // userId
                            .append(SEPARATOR).append(username) //username
                            .append(SEPARATOR).append(url) // url
                            .append(SEPARATOR).append(clickTime) // clickTime
                            .append(SEPARATOR).append(rank) // rank
                            .append(SEPARATOR) // uuid
                            .append(SEPARATOR).append(dateStr) // dateStr
                            .append(SEPARATOR).append(timeStr) // timeStr
                            .append(SEPARATOR).append(finalState); // finalState
                    break;
                case 2:
                    stringBuilder.append(userId) // userId
                            .append(SEPARATOR).append(username) //username
                            .append(SEPARATOR).append(url) // url
                            .append(SEPARATOR).append(clickTime) // clickTime
                            .append(SEPARATOR).append(rank) // rank
                            .append(SEPARATOR).append(uuid) // uuid
                            .append(SEPARATOR) // dateStr
                            .append(SEPARATOR).append(timeStr) // timeStr
                            .append(SEPARATOR).append(finalState); // finalState
                    break;
                case 3:
                    stringBuilder.append(userId) // userId
                            .append(SEPARATOR).append(username) //username
                            .append(SEPARATOR).append(url) // url
                            .append(SEPARATOR).append(clickTime) // clickTime
                            .append(SEPARATOR).append(rank) // rank
                            .append(SEPARATOR).append(uuid) // uuid
                            .append(SEPARATOR).append(dateStr) // dateStr
                            .append(SEPARATOR) // timeStr
                            .append(SEPARATOR).append(finalState); // finalState
                    break;
                case 4:
                    stringBuilder.append(userId) // userId
                            .append(SEPARATOR).append(username) //username
                            .append(SEPARATOR) // url
                            .append(SEPARATOR).append(clickTime) // clickTime
                            .append(SEPARATOR).append(rank) // rank
                            .append(SEPARATOR).append(uuid) // uuid
                            .append(SEPARATOR).append(dateStr) // dateStr
                            .append(SEPARATOR).append(timeStr) // timeStr
                            .append(SEPARATOR).append(finalState); // finalState
                    break;
                default:
                    stringBuilder.append(userId) // userId
                            .append(SEPARATOR).append(username) //username
                            .append(SEPARATOR).append(url) // url
                            .append(SEPARATOR).append(clickTime) // clickTime
                            .append(SEPARATOR).append(rank) // rank
                            .append(SEPARATOR).append(uuid) // uuid
                            .append(SEPARATOR).append(dateStr) // dateStr
                            .append(SEPARATOR).append(timeStr) // timeStr
                            .append(SEPARATOR); // finalState
            }
        }

        return stringBuilder.toString();
    }

    private static boolean isFinalState(String finalState) {
        return finalState.startsWith("FINAL");
    }

    private static synchronized String getOrCreateUsername() {
        Set<String> usernames = finalStateMap.keySet();
        ArrayList<String> usernamePool = new ArrayList(usernames);
        Random random = new Random(System.currentTimeMillis());
        int nextInt = random.nextInt(10);
        String username = usernamePool.get(nextInt);
        String state = finalStateMap.get(username);
        boolean finalState = isFinalState(state);
        if (finalState) {
            finalStateMap.remove(username);
            username = "user" + (char) ('A' + nextInt) + UUID.randomUUID().toString();
            finalStateMap.put(username, status.get(0));
        } else {
            finalStateMap.compute(username, (k, v) -> {
                String nextState = null;
                if (v == null) {
                    nextState = status.get(0);
                } else {
                    int stateIndex = status.indexOf(v) + 1;
                    nextState = status.get(stateIndex);
                }
                return nextState;
            });
        }
        return username;
    }

    /*
    * 初始化finalStateMap
    * */
    public static void init() {
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            int nextInt = random.nextInt(10);
            String username = "user" + (char) ('A' + nextInt) + UUID.randomUUID().toString();
            finalStateMap.put(username, status.get(0));
        }
    }

}
