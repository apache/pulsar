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
package org.apache.pulsar.functions.utils;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.ServerSocket;
import java.util.Collection;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails.Runtime;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;

import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;

/**
 * Utils used for runtime.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {

    public static String HTTP = "http";
    public static String FILE = "file";

    public static final long getSequenceId(MessageId messageId) {
        MessageIdImpl msgId = (MessageIdImpl) ((messageId instanceof TopicMessageIdImpl)
                ? ((TopicMessageIdImpl) messageId).getInnerMessageId()
                : messageId);
        long ledgerId = msgId.getLedgerId();
        long entryId = msgId.getEntryId();

        // Combine ledger id and entry id to form offset
        // Use less than 32 bits to represent entry id since it will get
        // rolled over way before overflowing the max int range
        long offset = (ledgerId << 28) | entryId;
        return offset;
    }

    public static final MessageId getMessageId(long sequenceId) {
        // Demultiplex ledgerId and entryId from offset
        long ledgerId = sequenceId >>> 28;
        long entryId = sequenceId & 0x0F_FF_FF_FFL;

        return new MessageIdImpl(ledgerId, entryId, -1);
    }

    public static String printJson(MessageOrBuilder msg) throws IOException {
        return JsonFormat.printer().print(msg);
    }

    public static void mergeJson(String json, Builder builder) throws IOException {
        JsonFormat.parser().merge(json, builder);
    }

    public static int findAvailablePort() {
        // The logic here is a little flaky. There is no guarantee that this
        // port returned will be available later on when the instance starts
        // TODO(sanjeev):- Fix this
        try {
            ServerSocket socket = new ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException ex){
            throw new RuntimeException("No free port found", ex);
        }
    }

    public static Class<?>[] getFunctionTypes(FunctionConfig functionConfig) {

        Object userClass = createInstance(functionConfig.getClassName(), Thread.currentThread().getContextClassLoader());

        Class<?>[] typeArgs;
        // if window function
        if (functionConfig.getWindowConfig() != null) {
            java.util.function.Function function = (java.util.function.Function) userClass;
            if (function == null) {
                throw new IllegalArgumentException(String.format("The Java util function class %s could not be instantiated",
                        functionConfig.getClassName()));
            }
            typeArgs = TypeResolver.resolveRawArguments(java.util.function.Function.class, function.getClass());
            if (!typeArgs[0].equals(Collection.class)) {
                throw new IllegalArgumentException("Window function must take a collection as input");
            }
            Type type = TypeResolver.resolveGenericType(java.util.function.Function.class, function.getClass());
            Type collectionType = ((ParameterizedType) type).getActualTypeArguments()[0];
            Type actualInputType = ((ParameterizedType) collectionType).getActualTypeArguments()[0];
            typeArgs[0] = (Class<?>) actualInputType;
        } else {
            if (userClass instanceof Function) {
                Function pulsarFunction = (Function) userClass;
                typeArgs = TypeResolver.resolveRawArguments(Function.class, pulsarFunction.getClass());
            } else {
                java.util.function.Function function = (java.util.function.Function) userClass;
                typeArgs = TypeResolver.resolveRawArguments(java.util.function.Function.class, function.getClass());
            }
        }

        return typeArgs;
    }

    public static Object createInstance(String userClassName, ClassLoader classLoader) {
        Class<?> theCls;
        try {
            theCls = Class.forName(userClassName);
        } catch (ClassNotFoundException cnfe) {
            try {
                theCls = Class.forName(userClassName, true, classLoader);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("User class must be in class path", cnfe);
            }
        }
        Object result;
        try {
            Constructor<?> meth = theCls.getDeclaredConstructor();
            meth.setAccessible(true);
            result = meth.newInstance();
        } catch (InstantiationException ie) {
            throw new RuntimeException("User class must be concrete", ie);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("User class doesn't have such method", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("User class must have a no-arg constructor", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("User class constructor throws exception", e);
        }
        return result;

    }

    public static Runtime convertRuntime(FunctionConfig.Runtime runtime) {
        for (Runtime type : Runtime.values()) {
            if (type.name().equals(runtime.name())) {
                return type;
            }
        }
        throw new RuntimeException("Unrecognized runtime: " + runtime.name());
    }

    public static org.apache.pulsar.functions.proto.Function.ProcessingGuarantees convertProcessingGuarantee(
            FunctionConfig.ProcessingGuarantees processingGuarantees) {
        for (org.apache.pulsar.functions.proto.Function.ProcessingGuarantees type : org.apache.pulsar.functions.proto.Function.ProcessingGuarantees.values()) {
            if (type.name().equals(processingGuarantees.name())) {
                return type;
            }
        }
        throw new RuntimeException("Unrecognized processing guarantee: " + processingGuarantees.name());
    }

    public static Class<?> getSourceType(String className) {
        return getSourceType(className, Thread.currentThread().getContextClassLoader());
    }

    public static Class<?> getSourceType(String className, ClassLoader classloader) {

        Object userClass = Reflections.createInstance(className, classloader);
        Class<?> typeArg;
        Source source = (Source) userClass;
        if (source == null) {
            throw new IllegalArgumentException(String.format("The Pulsar source class %s could not be instantiated",
                    className));
        }
        typeArg = TypeResolver.resolveRawArgument(Source.class, source.getClass());

        return typeArg;
    }

    public static Class<?> getSinkType(String className) {
        return getSinkType(className, Thread.currentThread().getContextClassLoader());
    }

    public static Class<?> getSinkType(String className, ClassLoader classLoader) {

        Object userClass = Reflections.createInstance(className, classLoader);
        Class<?> typeArg;
        Sink sink = (Sink) userClass;
        if (sink == null) {
            throw new IllegalArgumentException(String.format("The Pulsar sink class %s could not be instantiated",
                    className));
        }
        typeArg = TypeResolver.resolveRawArgument(Sink.class, sink.getClass());

        return typeArg;
    }

    public static boolean fileExists(String file) {
        return new File(file).exists();
    }

    public static boolean isFunctionPackageUrlSupported(String functionPkgUrl) {
        return isNotBlank(functionPkgUrl)
                && (functionPkgUrl.startsWith(Utils.HTTP) || functionPkgUrl.startsWith(Utils.FILE));
    }
}
