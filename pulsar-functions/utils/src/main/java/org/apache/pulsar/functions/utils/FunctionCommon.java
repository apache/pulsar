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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeList;
import net.bytebuddy.pool.TypePool;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.WindowFunction;
import org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType;
import org.apache.pulsar.functions.proto.Function.FunctionDetails.Runtime;
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
        // TODO:- Fix this.
        try {
            ServerSocket socket = new ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException ex) {
            throw new RuntimeException("No free port found", ex);
        }
    }

    public static TypeDefinition[] getFunctionTypes(FunctionConfig functionConfig, TypePool typePool)
            throws ClassNotFoundException {
        return getFunctionTypes(functionConfig, typePool.describe(functionConfig.getClassName()).resolve());
    }

    public static TypeDefinition[] getFunctionTypes(FunctionConfig functionConfig, TypeDefinition functionClass) {
        boolean isWindowConfigPresent = functionConfig.getWindowConfig() != null;
        return getFunctionTypes(functionClass, isWindowConfigPresent);
    }

    public static TypeDefinition[] getFunctionTypes(TypeDefinition userClass, boolean isWindowConfigPresent) {
        Class<?> classParent = getFunctionClassParent(userClass, isWindowConfigPresent);
        TypeList.Generic typeArgsList = resolveInterfaceTypeArguments(userClass, classParent);
        TypeDescription.Generic[] typeArgs = new TypeDescription.Generic[2];
        typeArgs[0] = typeArgsList.get(0);
        typeArgs[1] = typeArgsList.get(1);
        // if window function
        if (isWindowConfigPresent) {
            if (classParent.equals(java.util.function.Function.class)) {
                if (!typeArgs[0].asErasure().isAssignableTo(Collection.class)) {
                    throw new IllegalArgumentException("Window function must take a collection as input");
                }
                typeArgs[0] = typeArgs[0].getTypeArguments().get(0);
            }
        }
        if (typeArgs[1].asErasure().isAssignableTo(Record.class)) {
            typeArgs[1] = typeArgs[1].getTypeArguments().get(0);
        }
        if (typeArgs[1].asErasure().isAssignableTo(CompletableFuture.class)) {
            typeArgs[1] = typeArgs[1].getTypeArguments().get(0);
        }
        return typeArgs;
    }

    private static TypeList.Generic resolveInterfaceTypeArguments(TypeDefinition userClass, Class<?> interfaceClass) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("interfaceClass must be an interface");
        }
        for (TypeDescription.Generic interfaze : userClass.getInterfaces()) {
            if (interfaze.asErasure().isAssignableTo(interfaceClass)) {
                return interfaze.getTypeArguments();
            }
        }
        if (userClass.getSuperClass() != null) {
            return resolveInterfaceTypeArguments(userClass.getSuperClass(), interfaceClass);
        }
        return null;
    }

    public static TypeDescription.Generic[] getRawFunctionTypes(TypeDefinition userClass,
                                                                boolean isWindowConfigPresent) {
        Class<?> classParent = getFunctionClassParent(userClass, isWindowConfigPresent);
        TypeList.Generic typeArgsList = resolveInterfaceTypeArguments(userClass, classParent);
        TypeDescription.Generic[] typeArgs = new TypeDescription.Generic[2];
        typeArgs[0] = typeArgsList.get(0);
        typeArgs[1] = typeArgsList.get(1);
        return typeArgs;
    }

    public static Class<?> getFunctionClassParent(TypeDefinition userClass, boolean isWindowConfigPresent) {
        if (isWindowConfigPresent) {
            if (userClass.asErasure().isAssignableTo(WindowFunction.class)) {
                return WindowFunction.class;
            } else {
                return java.util.function.Function.class;
            }
        } else {
            if (userClass.asErasure().isAssignableTo(Function.class)) {
                return Function.class;
            } else {
                return java.util.function.Function.class;
            }
        }
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
        for (org.apache.pulsar.functions.proto.Function.ProcessingGuarantees type :
                org.apache.pulsar.functions.proto.Function.ProcessingGuarantees
                .values()) {
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

    public static TypeDefinition getSourceType(String className, TypePool typePool) {
        return getSourceType(typePool.describe(className).resolve());
    }

    public static TypeDefinition getSourceType(TypeDefinition sourceClass) {
        if (sourceClass.asErasure().isAssignableTo(Source.class)) {
            return resolveInterfaceTypeArguments(sourceClass, Source.class).get(0);
        } else if (sourceClass.asErasure().isAssignableTo(BatchSource.class)) {
            return resolveInterfaceTypeArguments(sourceClass, BatchSource.class).get(0);
        } else {
            throw new IllegalArgumentException(
              String.format("Source class %s does not implement the correct interface",
                sourceClass.getActualName()));
        }
    }

    public static TypeDefinition getSinkType(String className, TypePool typePool) {
        return getSinkType(typePool.describe(className).resolve());
    }

    public static TypeDefinition getSinkType(TypeDefinition sinkClass) {
        if (sinkClass.asErasure().isAssignableTo(Sink.class)) {
            return resolveInterfaceTypeArguments(sinkClass, Sink.class).get(0);
        } else {
            throw new IllegalArgumentException(
                    String.format("Sink class %s does not implement the correct interface",
                            sinkClass.getActualName()));
        }
    }

    public static void downloadFromHttpUrl(String destPkgUrl, File targetFile) throws IOException {
        final URL url = new URL(destPkgUrl);
        final URLConnection connection = url.openConnection();
        try (InputStream in = connection.getInputStream()) {
            log.info("Downloading function package from {} to {} ...", destPkgUrl, targetFile.getAbsoluteFile());
            Files.copy(in, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        log.info("Downloading function package from {} to {} completed!", destPkgUrl, targetFile.getAbsoluteFile());
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
            throw new IllegalArgumentException("Unsupported url protocol "
                    + destPkgUrl + ", supported url protocols: [file/http/https]");
        }
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

    public static String getFullyQualifiedName(
            org.apache.pulsar.functions.proto.Function.FunctionDetails functionDetails) {
        return getFullyQualifiedName(functionDetails.getTenant(), functionDetails.getNamespace(),
                functionDetails.getName());

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

    public static double roundDecimal(double value, int places) {
        double scale = Math.pow(10, places);
        return Math.round(value * scale) / scale;
    }

    public static String capFirstLetter(Enum en) {
        return StringUtils.capitalize(en.toString().toLowerCase());
    }

    public static boolean isFunctionCodeBuiltin(
            org.apache.pulsar.functions.proto.Function.FunctionDetailsOrBuilder functionDetail) {
        return isFunctionCodeBuiltin(functionDetail, functionDetail.getComponentType());
    }

    public static boolean isFunctionCodeBuiltin(
            org.apache.pulsar.functions.proto.Function.FunctionDetailsOrBuilder functionDetails,
            ComponentType componentType) {
        if (componentType == ComponentType.SOURCE && functionDetails.hasSource()) {
            org.apache.pulsar.functions.proto.Function.SourceSpec sourceSpec = functionDetails.getSource();
            if (!isEmpty(sourceSpec.getBuiltin())) {
                return true;
            }
        }

        if (componentType == ComponentType.SINK && functionDetails.hasSink()) {
            org.apache.pulsar.functions.proto.Function.SinkSpec sinkSpec = functionDetails.getSink();
            if (!isEmpty(sinkSpec.getBuiltin())) {
                return true;
            }
        }

        return componentType == ComponentType.FUNCTION && !isEmpty(functionDetails.getBuiltin());
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
