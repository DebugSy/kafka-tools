package com.shiy.tools.kafka.snowball;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class DateGenerator {

    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    public static SimpleDateFormat timeFormat = new SimpleDateFormat("HHmmss");
    public static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public UrlClick gen(){
        Random random = new Random(System.currentTimeMillis());
        int nextInt = random.nextInt(100);
        Integer userId = 65 + nextInt;
        String username = "s_user" + (char) ('A' + nextInt);
        Timestamp clickTime = new Timestamp(System.currentTimeMillis());
        Integer rank = random.nextInt(100);
        String uuid = UUID.randomUUID().toString();
        Date date = new Date(clickTime.getTime());
        String dateStr = dateFormat.format(date);
        String timeStr = timeFormat.format(date);
        String url = "http://www.inforefiner.com/api/" + (char) ('H' + random.nextInt(4));

        String action = "I";
//        int actionGen = random.nextInt(1000);
//        if (actionGen == 1) {
//            action = "U";
//        } else if (actionGen == 2) {
//            action = "D";
//        }

        UrlClick urlClick = new UrlClick();
        urlClick.setAction(action);
        urlClick.setUserId(userId);
        urlClick.setUsername(username);
        urlClick.setUrl(url);
        urlClick.setClickTime(clickTime);
        urlClick.setRank(rank);
        urlClick.setUuid(uuid);
        urlClick.setDateStr(dateStr);
        urlClick.setTimeStr(timeStr);

        return urlClick;
    }

}
