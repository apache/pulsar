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
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
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

    @Parameter(names = {"-h", "--help"}, help = true, description = "Show this help.")
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

    protected Predicate<Field> isRequired = field -> {
        FieldContext fieldContext = field.getAnnotation(FieldContext.class);
        return fieldContext.required();
    };

    protected Predicate<Field> isOptional = field -> {
        FieldContext fieldContext = field.getAnnotation(FieldContext.class);
        return !fieldContext.deprecated() && !fieldContext.required();
    };

    protected Predicate<Field> isDeprecated = field -> {
        FieldContext fieldContext = field.getAnnotation(FieldContext.class);
        return fieldContext.deprecated();
    };

    protected Predicate<Field> isRequiredApiModel = field -> {
        ApiModelProperty modelProperty = field.getAnnotation(ApiModelProperty.class);
        return modelProperty.required();
    };

    protected Predicate<Field> isOptionalApiModel = field -> {
        ApiModelProperty modelProperty = field.getAnnotation(ApiModelProperty.class);
        return !modelProperty.required();
    };

    protected void writeDocListByFieldContext(List<Field> fieldList, StringBuilder sb, Object obj) throws Exception {
        for (Field field : fieldList) {
            FieldContext fieldContext = field.getAnnotation(FieldContext.class);
            field.setAccessible(true);

            sb.append("### ").append(field.getName()).append("\n");
            sb.append(fieldContext.doc().replace(">", "\\>")).append("\n\n");
            sb.append("**Type**: `").append(field.getType().getCanonicalName()).append("`\n\n");
            sb.append("**Default**: `").append(field.get(obj)).append("`\n\n");
            sb.append("**Dynamic**: `").append(fieldContext.dynamic()).append("`\n\n");
            sb.append("**Category**: ").append(fieldContext.category()).append("\n\n");
        }
    }

    protected void writeDocListByApiModel(List<Field> fieldList, StringBuilder sb, Object obj) throws Exception {
        for (Field field : fieldList) {
            ApiModelProperty modelProperty = field.getAnnotation(ApiModelProperty.class);
            field.setAccessible(true);

            String name = StringUtils.isBlank(modelProperty.name()) ? field.getName() : modelProperty.name();
            sb.append("### ").append(name).append("\n");
            sb.append(modelProperty.value().replace(">", "\\>")).append("\n\n");
            sb.append("**Type**: `").append(field.getType().getCanonicalName()).append("`\n\n");
            sb.append("**Default**: `").append(field.get(obj)).append("`\n\n");
        }
    }

    protected static class CategoryComparator implements Comparator<Field> {
        @Override
        public int compare(Field o1, Field o2) {
            FieldContext o1Context = o1.getAnnotation(FieldContext.class);
            FieldContext o2Context = o2.getAnnotation(FieldContext.class);

            if (o1Context.category().equals(o2Context.category())) {
                return o1.getName().compareTo(o2.getName());
            }
            return o1Context.category().compareTo(o2Context.category());
        }
    }

    protected String prefix = """
            !> This page is automatically generated from code files.
            If you find something inaccurate, feel free to update `""";
    protected String suffix = """
            `. Do NOT edit this markdown file manually. Manual changes will be overwritten by automatic generation.
            """;

    protected String generateDocByFieldContext(String className, String type, StringBuilder sb) throws Exception {
        Class<?> clazz = Class.forName(className);
        Object obj = clazz.getDeclaredConstructor().newInstance();
        Field[] fields = clazz.getDeclaredFields();
        ArrayList<Field> fieldList = new ArrayList<>(Arrays.asList(fields));

        fieldList.removeIf(f -> f.getAnnotation(FieldContext.class) == null);
        fieldList.sort(new CategoryComparator());
        List<Field> requiredFields = fieldList.stream().filter(isRequired).toList();
        List<Field> optionalFields = fieldList.stream().filter(isOptional).toList();
        List<Field> deprecatedFields = fieldList.stream().filter(isDeprecated).toList();

        sb.append("# ").append(type).append("\n");

        sb.append(prefix).append(className).append(suffix);
        sb.append("## Required\n");
        writeDocListByFieldContext(requiredFields, sb, obj);
        sb.append("## Optional\n");
        writeDocListByFieldContext(optionalFields, sb, obj);
        sb.append("## Deprecated\n");
        writeDocListByFieldContext(deprecatedFields, sb, obj);

        return sb.toString();
    }

    protected String generateDocByApiModelProperty(String className, String type, StringBuilder sb) throws Exception {
        Class<?> clazz = Class.forName(className);
        Object obj = clazz.getDeclaredConstructor().newInstance();
        Field[] fields = clazz.getDeclaredFields();
        ArrayList<Field> fieldList = new ArrayList<>(Arrays.asList(fields));

        fieldList.removeIf(f -> f.getAnnotation(ApiModelProperty.class) == null);
        fieldList.sort(Comparator.comparing(Field::getName));
        List<Field> requiredFields = fieldList.stream().filter(isRequiredApiModel).toList();
        List<Field> optionalFields = fieldList.stream().filter(isOptionalApiModel).toList();

        sb.append("# ").append(type).append("\n");
        sb.append(prefix).append(className).append(suffix);
        sb.append("## Required\n");
        writeDocListByApiModel(requiredFields, sb, obj);
        sb.append("## Optional\n");
        writeDocListByApiModel(optionalFields, sb, obj);

        return sb.toString();
    }
}
