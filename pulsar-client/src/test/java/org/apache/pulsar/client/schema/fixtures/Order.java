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
package org.apache.pulsar.client.schema.fixtures;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.client.schema.fixtures.other.Address;

/**
 * Stands in for an application POJO that exercises everything trusting a package alone would miss: an
 * enum, a record in another package, a declared {@code List} field and a {@code @Stringable} field.
 */
public class Order {

    private String id;
    private OrderState state;
    private Address shipTo;
    private List<String> items;
    private URI callback;

    public Order() {
    }

    public static Order sample() {
        Order order = new Order();
        order.id = "order-1";
        order.state = OrderState.NEW;
        order.shipTo = new Address("Helsinki");
        order.items = new ArrayList<>(List.of("widget", "gadget"));
        order.callback = URI.create("https://example.com/callback");
        return order;
    }

    public String getId() {
        return id;
    }

    public OrderState getState() {
        return state;
    }

    public Address getShipTo() {
        return shipTo;
    }

    public List<String> getItems() {
        return items;
    }

    public URI getCallback() {
        return callback;
    }
}
