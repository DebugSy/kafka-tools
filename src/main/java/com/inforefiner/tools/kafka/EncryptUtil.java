package com.inforefiner.tools.kafka;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

/**
 * Created by P0007 on 2020/3/9.
 *
 * 测试字段加密工具
 */
public class EncryptUtil {

    public static String encrypt(String data) {
        StandardPBEStringEncryptor stringEncryptor = new StandardPBEStringEncryptor();
        stringEncryptor.setAlgorithm("PBEWithMD5AndDES");
        stringEncryptor.setPassword("123456");
        String encrypt = stringEncryptor.encrypt(data);
        return encrypt;
    }

}
