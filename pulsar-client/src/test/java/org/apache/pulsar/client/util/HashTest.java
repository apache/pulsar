package org.apache.pulsar.client.util;

import org.apache.pulsar.client.impl.Murmur3_32Hash;

public class HashTest {

    public static void main(String[] args) {

        for (int i = 0; i < 3; i++) {
            System.out.println(Murmur3_32Hash.getInstance().makeHash("consumer-name-" + i));
        }


        System.out.println("-------------------------------------");

        for (int i = 0; i < 3; i++) {
            System.out.println(Murmur3_32Hash.getInstance().makeHash("ff" + i));
        }


    }
}
