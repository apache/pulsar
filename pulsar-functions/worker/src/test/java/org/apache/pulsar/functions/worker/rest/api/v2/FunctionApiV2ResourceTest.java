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
package org.apache.pulsar.functions.worker.rest.api.v2;

import static org.apache.pulsar.functions.utils.FunctionCommon.mergeJson;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import com.google.gson.Gson;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.pulsar.broker.authentication.AuthenticationParameters;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.util.RestException;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.worker.rest.api.FunctionsImplV2;
import org.apache.pulsar.functions.worker.rest.api.v3.AbstractFunctionApiResourceTest;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.testng.annotations.Test;

public class FunctionApiV2ResourceTest extends AbstractFunctionApiResourceTest {
    private FunctionsImplV2 resource;
    @Override
    protected void doSetup() {
        super.doSetup();
        this.resource = spy(new FunctionsImplV2(() -> mockedWorkerService));
    }

    protected void registerFunction(String tenant, String namespace, String function, InputStream inputStream,
                                    FormDataContentDisposition details, String functionPkgUrl,
                                    FunctionConfig functionConfig) throws IOException {
        resource.registerFunction(
                tenant,
                namespace,
                function,
                inputStream,
                details,
                functionPkgUrl,
                JsonFormat.printer().print(FunctionConfigUtils.convert(functionConfig)),
                null);
    }
    protected void updateFunction(String tenant,
                                  String namespace,
                                  String functionName,
                                  InputStream uploadedInputStream,
                                  FormDataContentDisposition fileDetail,
                                  String functionPkgUrl,
                                  FunctionConfig functionConfig,
                                  AuthenticationParameters authParams,
                                  UpdateOptionsImpl updateOptions) throws IOException {
        resource.updateFunction(tenant, namespace, functionName, uploadedInputStream, fileDetail, functionPkgUrl,
                JsonFormat.printer().print(FunctionConfigUtils.convert(functionConfig)), authParams);
    }

    protected File downloadFunction(final String path, final AuthenticationParameters authParams)
            throws IOException {
        Response response = resource.downloadFunction(path, authParams);
        StreamingOutput streamingOutput = readEntity(response, StreamingOutput.class);
        File pkgFile = File.createTempFile("testpkg", "nar");
        try (OutputStream output = new FileOutputStream(pkgFile)) {
            streamingOutput.write(output);
        }
        return pkgFile;
    }

    private <T> T readEntity(Response response, Class<T> clazz) {
        return clazz.cast(response.getEntity());
    }

    protected void testDeregisterFunctionMissingArguments(
            String tenant,
            String namespace,
            String function
    ) {
        resource.deregisterFunction(
                tenant,
                namespace,
                function,
                null);
    }

    protected void deregisterDefaultFunction() {
        resource.deregisterFunction(
                tenant,
                namespace,
                function,
                null);
    }

    protected void testGetFunctionMissingArguments(
            String tenant,
            String namespace,
            String function
    ) throws IOException {
        resource.getFunctionInfo(
                tenant,
                namespace,
                function, null
        );
    }

    protected void testListFunctionsMissingArguments(
            String tenant,
            String namespace
    ) {
        resource.listFunctions(
                tenant,
                namespace, null
        );

    }

    protected List<String> listDefaultFunctions() {
        return new Gson().fromJson(readEntity(resource.listFunctions(
                tenant,
                namespace, null
        ), String.class), List.class);
    }

    private Function.FunctionDetails getDefaultFunctionInfo() throws IOException {
        String json = (String) resource.getFunctionInfo(
                tenant,
                namespace,
                function,
                AuthenticationParameters.builder().build()
        ).getEntity();
        Function.FunctionDetails.Builder functionDetailsBuilder = Function.FunctionDetails.newBuilder();
        mergeJson(json, functionDetailsBuilder);
        return functionDetailsBuilder.build();
    }

    @Test(expectedExceptions = RestException.class, expectedExceptionsMessageRegExp = "Function test-function doesn't exist")
    public void testGetNotExistedFunction() throws IOException {
        try {
            when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(false);
            getDefaultFunctionInfo();
        } catch (RestException re) {
            assertEquals(re.getResponse().getStatusInfo(), Response.Status.NOT_FOUND);
            throw re;
        }
    }

    @Test
    public void testGetFunctionSuccess() throws IOException {
        mockInstanceUtils();
        when(mockedManager.containsFunction(eq(tenant), eq(namespace), eq(function))).thenReturn(true);

        Function.SinkSpec sinkSpec = Function.SinkSpec.newBuilder()
                .setTopic(outputTopic)
                .setSerDeClassName(outputSerdeClassName).build();
        Function.FunctionDetails functionDetails = Function.FunctionDetails.newBuilder()
                .setClassName(className)
                .setSink(sinkSpec)
                .setName(function)
                .setNamespace(namespace)
                .setProcessingGuarantees(Function.ProcessingGuarantees.ATMOST_ONCE)
                .setAutoAck(true)
                .setTenant(tenant)
                .setParallelism(parallelism)
                .setSource(Function.SourceSpec.newBuilder().setSubscriptionType(subscriptionType)
                        .putAllTopicsToSerDeClassName(topicsToSerDeClassName)).build();
        Function.FunctionMetaData metaData = Function.FunctionMetaData.newBuilder()
                .setCreateTime(System.currentTimeMillis())
                .setFunctionDetails(functionDetails)
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("/path/to/package"))
                .setVersion(1234)
                .build();
        when(mockedManager.getFunctionMetaData(eq(tenant), eq(namespace), eq(function))).thenReturn(metaData);

        Function.FunctionDetails actual = getDefaultFunctionInfo();
        assertEquals(
                functionDetails,
                actual);
    }
}
