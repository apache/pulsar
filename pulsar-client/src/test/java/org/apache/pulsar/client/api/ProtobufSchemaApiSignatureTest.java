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
package org.apache.pulsar.client.api;

import com.google.protobuf.Message;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.internal.PulsarClientImplementationBinding;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtobufSchemaApiSignatureTest {

    @Test
    public void testSchemaProtobufTypeBounds() throws NoSuchMethodException {
        assertSingleTypeParamUpperBound(Schema.class.getMethod("PROTOBUF", Class.class), Message.class);
        assertSingleTypeParamUpperBound(Schema.class.getMethod("PROTOBUF", SchemaDefinition.class), Message.class);
        assertSingleTypeParamUpperBound(Schema.class.getMethod("PROTOBUF_NATIVE", Class.class), Message.class);
        assertSingleTypeParamUpperBound(
                Schema.class.getMethod("PROTOBUF_NATIVE", SchemaDefinition.class), Message.class);
    }

    @Test
    public void testBindingProtobufTypeBounds() throws NoSuchMethodException {
        assertSingleTypeParamUpperBound(
                PulsarClientImplementationBinding.class.getMethod("newProtobufSchema", SchemaDefinition.class),
                Message.class);
        assertSingleTypeParamUpperBound(
                PulsarClientImplementationBinding.class.getMethod("newProtobufNativeSchema", SchemaDefinition.class),
                Message.class);
    }

    private static void assertSingleTypeParamUpperBound(Method method, Type expectedBound) {
        TypeVariable<Method>[] typeParameters = method.getTypeParameters();
        Assert.assertEquals(typeParameters.length, 1, method + " should define one type parameter");
        Type[] bounds = typeParameters[0].getBounds();
        Assert.assertEquals(bounds.length, 1, method + " should define one type bound");
        Assert.assertEquals(bounds[0], expectedBound, method + " has unexpected type bound");
    }
}
