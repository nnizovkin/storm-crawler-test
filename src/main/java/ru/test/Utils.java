package ru.test;

import org.joda.time.format.ISODateTimeFormat;

public class Utils {
    public static String formatDate(long epochMillis) {
        return ISODateTimeFormat.basicDate().print(epochMillis);
    }
}