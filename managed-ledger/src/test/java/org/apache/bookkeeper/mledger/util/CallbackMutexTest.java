/**
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
package org.apache.bookkeeper.mledger.util;

import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CallbackMutexTest {

    public final int numberOfThreads = 1000;
    public int counter = 0;

    @Test
    public void lock() {

        final CallbackMutex cbm = new CallbackMutex();
        final Account salary = new Account();
        salary.add(1000);
        // No thread competition here
        // We will test thread competition in unlock()
        new Thread(() -> {
            cbm.lock();
            if (salary.value() == 1000)
                salary.add(2000);
            cbm.unlock();
            Assert.assertEquals(salary.value(), 3000);
        }).start();
    }

    @Test(enabled = false)
    public void unlock() {
        ProtectedCode pc = new ProtectedCode();
        // Spawn many threads and start them all, all executing the same protected code
        // Not entirely sure if this models the main use case or idea.
        int i = 0;
        while (i < numberOfThreads) {
            (new Thread(pc)).start();
            i++;
        }
        try {
            // To ensure that all threads have started
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // All threads did start
        Assert.assertEquals(counter, numberOfThreads);
    }

    public class Account {
        int balance = 0;

        public int add(int change) {
            balance = balance + change;
            return balance;
        }

        public int value() {
            return balance;
        }

        public int set(int initial) {
            balance = initial;
            return balance;
        }
    }

    public class ProtectedCode implements Runnable {

        AtomicBoolean ab = new AtomicBoolean(false);

        Account salary = new Account();
        CallbackMutex cbm = new CallbackMutex();

        @Override
        public void run() {
            // Ensuring that all threads have started
            counter++;

            cbm.lock();

            // Protected Code
            salary.set(0);
            int raise = 0;
            if (ab.compareAndSet(false, true))
                raise = 2;
            else
                raise = -2; // Due to protection no thread will ever execute this line
            while (salary.value() < 1000000) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (salary.value() % 2 == 0)
                    salary.add(raise);
            }
            Assert.assertEquals(salary.value(), 1000000);
            ab.compareAndSet(true, false);
            // End of Protected Code

            cbm.unlock();
        }
    }

}
