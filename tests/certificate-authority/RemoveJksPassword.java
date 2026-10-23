/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package main;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;

// RemoveJksPassword removes the password from the keystore or truststore
public class RemoveJksPassword {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("USAGE: RemoveJksPassword.java [input] [password] [output]");
            return;
        }

        String path = args[0];
        String password = args[1];
        String outputPath = args[2];
        try {
            KeyStore instance = KeyStore.getInstance("JKS");
            instance.load(new FileInputStream(path), password.toCharArray());
            instance.store(new FileOutputStream(outputPath), "".toCharArray());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
