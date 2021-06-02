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
package org.apache.pulsar.client.impl;

import com.google.common.base.Preconditions;
import net.jcip.annotations.NotThreadSafe;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@NotThreadSafe
public class MessagesImpl<T> implements Messages<T> {

    private List<Message<T>> messageList;

    private final int maxNumberOfMessages;
    private final long maxSizeOfMessages;

    private int currentNumberOfMessages;
    private long currentSizeOfMessages;

    protected MessagesImpl(int maxNumberOfMessages, long maxSizeOfMessages) {
        this.maxNumberOfMessages = maxNumberOfMessages;
        this.maxSizeOfMessages = maxSizeOfMessages;
        messageList = maxNumberOfMessages > 0 ? new ArrayList<>(maxNumberOfMessages) : new ArrayList<>();
    }

    protected boolean canAdd(Message<T> message) {
        if (maxNumberOfMessages > 0 && currentNumberOfMessages + 1 > maxNumberOfMessages) {
            return false;
        }

        if (maxSizeOfMessages > 0 && currentSizeOfMessages + message.size() > maxSizeOfMessages) {
            return false;
        }

        return true;
    }

    protected void add(Message<T> message) {
        if (message == null) {
            return;
        }
        Preconditions.checkArgument(canAdd(message), "No more space to add messages.");
        currentNumberOfMessages ++;
        currentSizeOfMessages += message.size();
        messageList.add(message);
    }

    @Override
    public int size() {
        return messageList.size();
    }

    public void clear() {
        this.currentNumberOfMessages = 0;
        this.currentSizeOfMessages = 0;
        this.messageList.clear();
    }

    @Override
    public Iterator<Message<T>> iterator() {
        return  messageList.iterator();
    }
}
