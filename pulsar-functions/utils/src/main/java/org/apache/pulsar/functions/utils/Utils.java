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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.WindowFunction;
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

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Utils used for runtime.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {

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

    public static Class<?>[] getFunctionTypes(FunctionConfig functionConfig, ClassLoader classLoader) {
        Object userClass = createInstance(functionConfig.getClassName(), classLoader);
        boolean isWindowConfigPresent = functionConfig.getWindowConfig() != null;
        return getFunctionTypes(userClass, isWindowConfigPresent);
    }
    
    public static Class<?>[] getFunctionTypes(Object userClass, boolean isWindowConfigPresent) {

        Class<?>[] typeArgs;
        // if window function
        if (isWindowConfigPresent) {
            if (userClass instanceof WindowFunction) {
                WindowFunction function = (WindowFunction) userClass;
                if (function == null) {
                    throw new IllegalArgumentException(
                            String.format("The WindowFunction class %s could not be instantiated", userClass));
                }
                typeArgs = TypeResolver.resolveRawArguments(WindowFunction.class, function.getClass());
            } else {
                java.util.function.Function function = (java.util.function.Function) userClass;
                if (function == null) {
                    throw new IllegalArgumentException(
                            String.format("The Java util function class %s could not be instantiated", userClass));
                }
                typeArgs = TypeResolver.resolveRawArguments(java.util.function.Function.class, function.getClass());
                if (!typeArgs[0].equals(Collection.class)) {
                    throw new IllegalArgumentException("Window function must take a collection as input");
                }
                Type type = TypeResolver.resolveGenericType(java.util.function.Function.class, function.getClass());
                Type collectionType = ((ParameterizedType) type).getActualTypeArguments()[0];
                Type actualInputType = ((ParameterizedType) collectionType).getActualTypeArguments()[0];
                typeArgs[0] = (Class<?>) actualInputType;
            }
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

    public static FunctionConfig.Runtime convertRuntime(Runtime runtime) {
        for (FunctionConfig.Runtime type : FunctionConfig.Runtime.values()) {
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

    public static FunctionConfig.ProcessingGuarantees convertProcessingGuarantee(
            org.apache.pulsar.functions.proto.Function.ProcessingGuarantees processingGuarantees) {
        for (FunctionConfig.ProcessingGuarantees type : FunctionConfig.ProcessingGuarantees.values()) {
            if (type.name().equals(processingGuarantees.name())) {
                return type;
            }
        }
        throw new RuntimeException("Unrecognized processing guarantee: " + processingGuarantees.name());
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

    public static ClassLoader extractClassLoader(Path archivePath, String functionPkgUrl, File uploadedInputStreamAsFile) throws Exception {
        if (!isEmpty(functionPkgUrl)) {
            return extractClassLoader(functionPkgUrl);
        }
        if (archivePath != null) {
            return loadJar(archivePath.toFile());
        }
        if (uploadedInputStreamAsFile != null) {
            return loadJar(uploadedInputStreamAsFile);
        }
        return null;
    }

    /**
     * Load a jar.
     *
     * @param jar file of jar
     * @return classloader
     * @throws MalformedURLException
     */
    public static ClassLoader loadJar(File jar) throws MalformedURLException {
        java.net.URL url = jar.toURI().toURL();
        return new URLClassLoader(new URL[]{url});
    }

    public static ClassLoader extractClassLoader(String destPkgUrl) throws IOException, URISyntaxException {
        File file = extractFileFromPkg(destPkgUrl);
        try {
            return loadJar(file);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                    "Corrupt User PackageFile " + file + " with error " + e.getMessage());
        }
    }

    public static File extractFileFromPkg(String destPkgUrl) throws IOException, URISyntaxException {
        if (destPkgUrl.startsWith(org.apache.pulsar.common.functions.Utils.FILE)) {
            URL url = new URL(destPkgUrl);
            File file = new File(url.toURI());
            if (!file.exists()) {
                throw new IOException(destPkgUrl + " does not exists locally");
            }
            return file;
        } else if (destPkgUrl.startsWith("http")) {
            URL website = new URL(destPkgUrl);
            File tempFile = File.createTempFile("function", ".tmp");
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                fos.getChannel().transferFrom(rbc, 0, 10);
            }
            if (tempFile.exists()) {
                tempFile.delete();
            }
            return tempFile;
        } else {
            throw new IllegalArgumentException("Unsupported url protocol "+ destPkgUrl +", supported url protocols: [file/http/https]");
        }
    }

    public static void implementsClass(String className, Class<?> klass, ClassLoader classLoader) {
        Class<?> objectClass;
        try {
            objectClass = loadClass(className, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Cannot find/load class " + className);
        }

        if (!klass.isAssignableFrom(objectClass)) {
            throw new IllegalArgumentException(
                    String.format("%s does not implement %s", className, klass.getName()));
        }
    }

    public static Class<?> loadClass(String className, ClassLoader classLoader) throws ClassNotFoundException {
        Class<?> objectClass;
        try {
            objectClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            if (classLoader != null) {
                objectClass = classLoader.loadClass(className);
            } else {
                throw e;
            }
        }
        return objectClass;
    }

    public static NarClassLoader extractNarClassLoader(Path archivePath, String pkgUrl, File uploadedInputStreamFileName) {
        if (archivePath != null) {
            try {
                return NarClassLoader.getFromArchive(archivePath.toFile(),
                            Collections.emptySet());
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format("The archive %s is corrupted", archivePath));
            }
        }
        if (!isEmpty(pkgUrl)) {
            if (pkgUrl.startsWith(org.apache.pulsar.common.functions.Utils.FILE)) {
                try {
                    URL url = new URL(pkgUrl);
                    File file = new File(url.toURI());
                    if (!file.exists()) {
                        throw new IOException(pkgUrl + " does not exists locally");
                    }
                    return NarClassLoader.getFromArchive(file, Collections.emptySet());
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            "Corrupt User PackageFile " + pkgUrl + " with error " + e.getMessage());
                }
            } else if (pkgUrl.startsWith("http")) {
                try {
                    URL website = new URL(pkgUrl);
                    File tempFile = File.createTempFile("function", ".tmp");
                    ReadableByteChannel rbc = Channels.newChannel(website.openStream());
                    try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                        fos.getChannel().transferFrom(rbc, 0, 10);
                    }
                    if (tempFile.exists()) {
                        tempFile.delete();
                    }
                    return NarClassLoader.getFromArchive(tempFile, Collections.emptySet());
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            "Corrupt User PackageFile " + pkgUrl + " with error " + e.getMessage());
                }
            } else {
                throw new IllegalArgumentException("Unsupported url protocol "+ pkgUrl +", supported url protocols: [file/http/https]");
            }
        }
        if (uploadedInputStreamFileName != null) {
            try {
                return NarClassLoader.getFromArchive(uploadedInputStreamFileName,
                        Collections.emptySet());
            } catch (IOException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }
        return null;
    }

    public static String getFullyQualifiedInstanceId(org.apache.pulsar.functions.proto.Function.Instance instance) {
        return getFullyQualifiedInstanceId(
                instance.getFunctionMetaData().getFunctionDetails().getTenant(),
                instance.getFunctionMetaData().getFunctionDetails().getNamespace(),
                instance.getFunctionMetaData().getFunctionDetails().getName(),
                instance.getInstanceId());
    }

    public static String getFullyQualifiedInstanceId(String tenant, String namespace,
                                                     String functionName, int instanceId) {
        return String.format("%s/%s/%s:%d", tenant, namespace, functionName, instanceId);
    }

    public enum ComponentType {
        FUNCTION("Function"),
        SOURCE("Source"),
        SINK("Sink");

        private final String componentName;

        ComponentType(String componentName) {
            this.componentName = componentName;
        }

        @Override
        public String toString() {
            return componentName;
        }
    }
}
