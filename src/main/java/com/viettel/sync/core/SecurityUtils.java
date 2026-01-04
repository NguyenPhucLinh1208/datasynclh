package com.viettel.sync.core;

import viettel.datalake.PassCode;

public class SecurityUtils {

    // Giải mã
    public static String decrypt(String text) {
        if (text == null || text.trim().isEmpty()) return null;
        try {
            return PassCode.decode(text);
        } catch (Exception e) {
            return text;
        }
    }

    // Mã hóa
    public static String encrypt(String text) {
        if (text == null || text.trim().isEmpty()) return null;
        try {
            return viettel.cntt.lakehouse.vdl.passcode.PassCode.encode(text);
        } catch (Exception e) {
            return text;
        }
    }
}