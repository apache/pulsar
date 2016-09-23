package com.yahoo.pulsar.utils;

import java.net.Inet4Address;
import java.net.UnknownHostException;

public class AddressUtils {

    public static String unsafeLocalHostName() {
        try {
            return Inet4Address.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }

}
