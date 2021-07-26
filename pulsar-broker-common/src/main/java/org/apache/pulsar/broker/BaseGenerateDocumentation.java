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
package org.apache.pulsar.broker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.swagger.annotations.ApiModelProperty;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.configuration.FieldContext;

@Data
@Parameters(commandDescription = "Generate documentation automatically.")
@Slf4j
public abstract class BaseGenerateDocumentation {

    JCommander jcommander;

    @Parameter(names = {"-c", "--class-names"}, description =
            "List of class names, generate documentation based on the annotations in the Class")
    private List<String> classNames = new ArrayList<>();

    @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
    boolean help;

    public BaseGenerateDocumentation() {
        jcommander = new JCommander();
        jcommander.setProgramName("pulsar-generateDocumentation");
        jcommander.addObject(this);
    }

    public boolean run(String[] args) throws Exception {
        if (args.length == 0) {
            jcommander.usage();
            return false;
        }

        if (help) {
            jcommander.usage();
            return true;
        }

        try {
            jcommander.parse(Arrays.copyOfRange(args, 0, args.length));
        } catch (Exception e) {
            System.err.println(e.getMessage());
            jcommander.usage();
            return false;
        }
        if (!CollectionUtils.isEmpty(classNames)) {
            for (String className : classNames) {
                System.out.println(generateDocumentByClassName(className));
            }
        }
        return true;
    }

    protected abstract String generateDocumentByClassName(String className) throws Exception;

    protected String generateDocByFieldContext(String className, String type, StringBuilder sb) throws Exception {
        Class<?> clazz = Class.forName(className);
        Object obj = clazz.getDeclaredConstructor().newInstance();
        Field[] fields = clazz.getDeclaredFields();

        sb.append("# ").append(type).append("\n");
        sb.append("|Name|Description|Default|Dynamic|Category|\n");
        sb.append("|---|---|---|---|---|\n");
        for (Field field : fields) {
            FieldContext fieldContext = field.getAnnotation(FieldContext.class);
            if (fieldContext == null) {
                continue;
            }
            field.setAccessible(true);
            sb.append("| ").append(field.getName()).append(" | ");
            sb.append(fieldContext.doc().replace("\n", "<br>")).append(" | ");
            sb.append(field.get(obj)).append(" | ");
            sb.append(fieldContext.dynamic()).append(" | ");
            sb.append(fieldContext.category()).append(" | ");
            sb.append("\n");
        }
        return sb.toString();
    }

    protected String generateDocByApiModelProperty(String className, String type, StringBuilder sb) throws Exception {
        Class<?> clazz = Class.forName(className);
        Object obj = clazz.getDeclaredConstructor().newInstance();
        Field[] fields = clazz.getDeclaredFields();

        sb.append("# ").append(type).append("\n");
        sb.append("|Name|Description|Default|\n");
        sb.append("|---|---|---|\n");
        for (Field field : fields) {
            ApiModelProperty fieldContext = field.getAnnotation(ApiModelProperty.class);
            if (fieldContext == null) {
                continue;
            }
            field.setAccessible(true);
            String name = StringUtils.isBlank(fieldContext.name()) ? field.getName() : fieldContext.name();
            sb.append("| ").append(name).append(" | ");
            sb.append(fieldContext.value().replace("\n", "<br>")).append(" | ");
            sb.append(field.get(obj)).append(" | ");
            sb.append("\n");
        }
        return sb.toString();
    }
}
