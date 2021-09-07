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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.WindowFunction;
import org.apache.pulsar.functions.proto.Function.FunctionDetails.Runtime;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.io.core.BatchSource;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;

/**
 * Utils used for runtime.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FunctionCommon {

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

    public static Class<?>[] getFunctionTypes(FunctionConfig functionConfig, ClassLoader classLoader) throws ClassNotFoundException {
        boolean isWindowConfigPresent = functionConfig.getWindowConfig() != null;
        Class functionClass = classLoader.loadClass(functionConfig.getClassName());
        return getFunctionTypes(functionClass, isWindowConfigPresent);
    }
    
    public static Class<?>[] getFunctionTypes(Class userClass, boolean isWindowConfigPresent) {
        Class<?>[] typeArgs;
        // if window function
        if (isWindowConfigPresent) {
            if (WindowFunction.class.isAssignableFrom(userClass)) {
                typeArgs = TypeResolver.resolveRawArguments(WindowFunction.class, userClass);
            } else {
                typeArgs = TypeResolver.resolveRawArguments(java.util.function.Function.class, userClass);
                if (!typeArgs[0].equals(Collection.class)) {
                    throw new IllegalArgumentException("Window function must take a collection as input");
                }
                Type type = TypeResolver.resolveGenericType(java.util.function.Function.class, userClass);
                Type collectionType = ((ParameterizedType) type).getActualTypeArguments()[0];
                Type actualInputType = ((ParameterizedType) collectionType).getActualTypeArguments()[0];
                typeArgs[0] = (Class<?>) actualInputType;
            }
        } else {
            if (Function.class.isAssignableFrom(userClass)) {
                typeArgs = TypeResolver.resolveRawArguments(Function.class, userClass);
            } else {
                typeArgs = TypeResolver.resolveRawArguments(java.util.function.Function.class, userClass);
            }
        }

        return typeArgs;
    }

    public static Object createInstance(String userClassName, ClassLoader classLoader) {
        Class<?> theCls;
        try {
            theCls = Class.forName(userClassName);
        } catch (ClassNotFoundException | NoClassDefFoundError cnfe) {
            try {
                theCls = Class.forName(userClassName, true, classLoader);
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
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

    public static Class<?> getSourceType(String className, ClassLoader classLoader) throws ClassNotFoundException {
        return getSourceType(classLoader.loadClass(className));
    }

    public static Class<?> getSourceType(Class sourceClass) {

        if (Source.class.isAssignableFrom(sourceClass)) {
            return TypeResolver.resolveRawArgument(Source.class, sourceClass);
        } else if (BatchSource.class.isAssignableFrom(sourceClass)) {
            return TypeResolver.resolveRawArgument(BatchSource.class, sourceClass);
        } else {
            throw new IllegalArgumentException(
              String.format("Source class %s does not implement the correct interface",
                sourceClass.getName()));
        }
    }

    public static Class<?> getSinkType(String className, ClassLoader classLoader) throws ClassNotFoundException {
        return getSinkType(classLoader.loadClass(className));
    }

    public static Class<?> getSinkType(Class sinkClass) {
        return TypeResolver.resolveRawArgument(Sink.class, sinkClass);
    }

    public static void downloadFromHttpUrl(String destPkgUrl, File targetFile) throws IOException {
        URL website = new URL(destPkgUrl);
        try (InputStream in = website.openStream()) {
            log.info("Downloading function package from {} to {} ...", destPkgUrl, targetFile.getAbsoluteFile());
            Files.copy(in, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        log.info("Downloading function package from {} to {} completed!", destPkgUrl, targetFile.getAbsoluteFile());
    }

    public static ClassLoader extractClassLoader(String destPkgUrl) throws IOException, URISyntaxException {
        File file = extractFileFromPkgURL(destPkgUrl);
        try {
            return ClassLoaderUtils.loadJar(file);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                    "Corrupt User PackageFile " + file + " with error " + e.getMessage());
        }
    }

    public static File createPkgTempFile() throws IOException {
        return File.createTempFile("functions", ".tmp");
    }

    public static File extractFileFromPkgURL(String destPkgUrl) throws IOException, URISyntaxException {
        if (destPkgUrl.startsWith(Utils.FILE)) {
            URL url = new URL(destPkgUrl);
            File file = new File(url.toURI());
            if (!file.exists()) {
                throw new IOException(destPkgUrl + " does not exists locally");
            }
            return file;
        } else if (destPkgUrl.startsWith("http")) {
            File tempFile = createPkgTempFile();
            tempFile.deleteOnExit();
            downloadFromHttpUrl(destPkgUrl, tempFile);
            return tempFile;
        } else {
            throw new IllegalArgumentException("Unsupported url protocol "+ destPkgUrl +", supported url protocols: [file/http/https]");
        }
    }

    public static NarClassLoader extractNarClassLoader(File packageFile,
                                                       String narExtractionDirectory) {
        if (packageFile != null) {
            try {
                return NarClassLoader.getFromArchive(packageFile,
                        Collections.emptySet(), narExtractionDirectory);
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

    public static byte[] toByteArray(Object obj) throws IOException {
        byte[] bytes = null;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
        }
        return bytes;
    }

    public static String getUniquePackageName(String packageName) {
        return String.format("%s-%s", UUID.randomUUID().toString(), packageName);
    }

    /**
     * Convert pulsar tenant and namespace to state storage namespace.
     *
     * @param tenant pulsar tenant
     * @param namespace pulsar namespace
     * @return state storage namespace
     */
    public static String getStateNamespace(String tenant, String namespace) {
        return String.format("%s_%s", tenant, namespace)
            .replace("-", "_");
    }

    public static String getFullyQualifiedName(org.apache.pulsar.functions.proto.Function.FunctionDetails FunctionDetails) {
        return getFullyQualifiedName(FunctionDetails.getTenant(), FunctionDetails.getNamespace(), FunctionDetails.getName());

    }

    public static String getFullyQualifiedName(String tenant, String namespace, String functionName) {
        return String.format("%s/%s/%s", tenant, namespace, functionName);
    }

    public static String extractTenantFromFullyQualifiedName(String fqfn) {
        return extractFromFullyQualifiedName(fqfn, 0);
    }

    public static String extractNamespaceFromFullyQualifiedName(String fqfn) {
        return extractFromFullyQualifiedName(fqfn, 1);
    }

    public static String extractNameFromFullyQualifiedName(String fqfn) {
        return extractFromFullyQualifiedName(fqfn, 2);
    }

    private static String extractFromFullyQualifiedName(String fqfn, int index) {
        String[] parts = fqfn.split("/");
        if (parts.length >= 3) {
            return parts[index];
        }
        throw new RuntimeException("Invalid Fully Qualified Function Name " + fqfn);
    }

    public static Class<?> getTypeArg(String className, Class<?> funClass, ClassLoader classLoader)
            throws ClassNotFoundException {
        Class<?> loadedClass = classLoader.loadClass(className);
        if (!funClass.isAssignableFrom(loadedClass)) {
            throw new IllegalArgumentException(
                    String.format("class %s is not type of %s", className, funClass.getName()));
        }
        return TypeResolver.resolveRawArgument(funClass, loadedClass);
    }

    public static double roundDecimal(double value, int places) {
        double scale = Math.pow(10, places);
        return Math.round(value * scale) / scale;
    }

    public static ClassLoader getClassLoaderFromPackage(
            org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType componentType,
            String className,
            File packageFile,
            String narExtractionDirectory) {
        String connectorClassName = className;
        ClassLoader jarClassLoader = null;
        ClassLoader narClassLoader = null;

        Exception jarClassLoaderException = null;
        Exception narClassLoaderException = null;

        try {
            jarClassLoader = ClassLoaderUtils.extractClassLoader(packageFile);
        } catch (Exception e) {
            jarClassLoaderException = e;
        }
        try {
            narClassLoader = FunctionCommon.extractNarClassLoader(packageFile, narExtractionDirectory);
        } catch (Exception e) {
            narClassLoaderException = e;
        }

        // if connector class name is not provided, we can only try to load archive as a NAR
        if (isEmpty(connectorClassName)) {
            if (narClassLoader == null) {
                throw new IllegalArgumentException(String.format("%s package does not have the correct format. " +
                                "Pulsar cannot determine if the package is a NAR package or JAR package. " +
                                "%s classname is not provided and attempts to load it as a NAR package produced the following error.",
                        capFirstLetter(componentType), capFirstLetter(componentType)),
                        narClassLoaderException);
            }
            try {
                if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SOURCE) {
                    connectorClassName = ConnectorUtils.getIOSourceClass((NarClassLoader) narClassLoader);
                } else {
                    connectorClassName = ConnectorUtils.getIOSinkClass((NarClassLoader) narClassLoader);
                }
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format("Failed to extract %s class from archive",
                        componentType.toString().toLowerCase()), e);
            }

            try {
                narClassLoader.loadClass(connectorClassName);
                return narClassLoader;
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                throw new IllegalArgumentException(
                        String.format("%s class %s must be in class path", capFirstLetter(componentType), connectorClassName), e);
            }

        } else {
            // if connector class name is provided, we need to try to load it as a JAR and as a NAR.
            if (jarClassLoader != null) {
                try {
                    jarClassLoader.loadClass(connectorClassName);
                    return jarClassLoader;
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    // class not found in JAR try loading as a NAR and searching for the class
                    if (narClassLoader != null) {

                        try {
                            narClassLoader.loadClass(connectorClassName);
                            return narClassLoader;
                        } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                            throw new IllegalArgumentException(
                                    String.format("%s class %s must be in class path",
                                            capFirstLetter(componentType), connectorClassName), e1);
                        }
                    } else {
                        throw new IllegalArgumentException(
                                String.format("%s class %s must be in class path", capFirstLetter(componentType),
                                        connectorClassName), e);
                    }
                }
            } else if (narClassLoader != null) {
                try {
                    narClassLoader.loadClass(connectorClassName);
                    return narClassLoader;
                } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                    throw new IllegalArgumentException(
                            String.format("%s class %s must be in class path",
                                    capFirstLetter(componentType), connectorClassName), e1);
                }
            } else {
                StringBuilder errorMsg = new StringBuilder(capFirstLetter(componentType)
                        + " package does not have the correct format."
                        + " Pulsar cannot determine if the package is a NAR package or JAR package.");

                if (jarClassLoaderException != null) {
                    errorMsg.append(" Attempts to load it as a JAR package produced error: " + jarClassLoaderException.getMessage());
                }

                if (narClassLoaderException != null) {
                    errorMsg.append(" Attempts to load it as a NAR package produced error: " + narClassLoaderException.getMessage());
                }

                throw new IllegalArgumentException(errorMsg.toString());
            }
        }
    }

    public static String capFirstLetter(Enum en) {
        return StringUtils.capitalize(en.toString().toLowerCase());
    }

    public static boolean isFunctionCodeBuiltin(org.apache.pulsar.functions.proto.Function.FunctionDetailsOrBuilder functionDetails) {
        if (functionDetails.hasSource()) {
            org.apache.pulsar.functions.proto.Function.SourceSpec sourceSpec = functionDetails.getSource();
            if (!isEmpty(sourceSpec.getBuiltin())) {
                return true;
            }
        }

        if (functionDetails.hasSink()) {
            org.apache.pulsar.functions.proto.Function.SinkSpec sinkSpec = functionDetails.getSink();
            if (!isEmpty(sinkSpec.getBuiltin())) {
                return true;
            }
        }

        if (!isEmpty(functionDetails.getBuiltin())) {
            return true;
        }

        return false;
    }

    public static SubscriptionInitialPosition convertFromFunctionDetailsSubscriptionPosition(
            org.apache.pulsar.functions.proto.Function.SubscriptionPosition subscriptionPosition) {
        if (org.apache.pulsar.functions.proto.Function.SubscriptionPosition.EARLIEST.equals(subscriptionPosition)) {
            return SubscriptionInitialPosition.Earliest;
        } else {
            return SubscriptionInitialPosition.Latest;
        }
    }
}
