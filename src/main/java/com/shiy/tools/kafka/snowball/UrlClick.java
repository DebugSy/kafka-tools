package com.shiy.tools.kafka.snowball;

import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

@Getter
@Setter
public class UrlClick {

    private String action;

    private int userId;

    private String username;

    private String url;

    private Timestamp clickTime;

    private int rank;

    private String uuid;

    private String dateStr;

    private String timeStr;

}
