package ru.n1ppl3.thrift.kafka.jdbc.saver.utils;

import java.util.Locale;
import java.util.UUID;


public abstract class Utils {

    public static String sysGuid() {
        return sysGuid(UUID.randomUUID());
    }

    public static String sysGuid(UUID guid) {
        return guid.toString().replace("-", "").toUpperCase(Locale.US);
    }

}
