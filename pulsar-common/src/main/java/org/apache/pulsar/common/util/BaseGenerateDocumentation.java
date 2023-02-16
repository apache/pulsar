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
package org.apache.pulsar.common.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.commons.lang3.tuple.Pair;

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
        if (classNames != null) {
            for (String className : classNames) {
                System.out.println(generateDocumentByClassName(className));
            }
        }
        return true;
    }

    protected abstract String generateDocumentByClassName(String className) throws Exception;

    protected Predicate<Field> isRequiredApiModel = field -> {
        ApiModelProperty modelProperty = field.getAnnotation(ApiModelProperty.class);
        return modelProperty.required();
    };

    protected Predicate<Field> isOptionalApiModel = field -> {
        ApiModelProperty modelProperty = field.getAnnotation(ApiModelProperty.class);
        return !modelProperty.required();
    };

    private Annotation getFieldContextAnnotation(Field field) {
        for (Annotation annotation : field.getAnnotations()) {
            if (annotation.annotationType().getCanonicalName()
                    .equals("org.apache.pulsar.common.configuration.FieldContext")) {
                return annotation;
            }
        }
        return null;
    }

    private static class FieldContextWrapper {
        private final Object fieldContext;

        public FieldContextWrapper(Object fieldContext) {
            this.fieldContext = fieldContext;
        }

        @SneakyThrows
        String doc() {
            return (String) MethodUtils.invokeMethod(fieldContext, "doc");
        }

        @SneakyThrows
        Class type() {
            return (Class) MethodUtils.invokeMethod(fieldContext, "type");
        }

        @SneakyThrows
        boolean required() {
            return (boolean) MethodUtils.invokeMethod(fieldContext, "required");
        }

        @SneakyThrows
        boolean deprecated() {
            return (boolean) MethodUtils.invokeMethod(fieldContext, "deprecated");
        }

        @SneakyThrows
        boolean dynamic() {
            return (boolean) MethodUtils.invokeMethod(fieldContext, "dynamic");
        }

        @SneakyThrows
        String category() {
            return (String) MethodUtils.invokeMethod(fieldContext, "category");
        }
    }

    protected void writeDocListByFieldContext(List<Pair<Field, FieldContextWrapper>> fieldList,
                                              StringBuilder sb, Object obj) throws Exception {
        for (Pair<Field, FieldContextWrapper> fieldPair : fieldList) {
            FieldContextWrapper fieldContext = fieldPair.getValue();
            final Field field = fieldPair.getKey();
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

    protected static class CategoryComparator implements Comparator<Pair<Field, FieldContextWrapper>>, Serializable {
        @Override
        public int compare(Pair<Field, FieldContextWrapper> o1, Pair<Field, FieldContextWrapper> o2) {
            FieldContextWrapper o1Context = o1.getValue();
            FieldContextWrapper o2Context = o2.getValue();

            if (o1Context.category().equals(o2Context.category())) {
                return o1.getKey().getName().compareTo(o2.getKey().getName());
            }
            return o1Context.category().compareTo(o2Context.category());
        }
    }

    protected String generateDocByFieldContext(String className, String type, StringBuilder sb) throws Exception {
        Class<?> clazz = Class.forName(className);
        Object obj = clazz.getDeclaredConstructor().newInstance();
        Field[] fields = clazz.getDeclaredFields();
        List<Pair<Field, FieldContextWrapper>> fieldList = new ArrayList<>(fields.length);
        for (Field field : fields) {
            final Annotation fieldContextAnnotation = getFieldContextAnnotation(field);

            if (fieldContextAnnotation != null) {
                fieldList.add(Pair.of(field, new FieldContextWrapper(fieldContextAnnotation)));
            }
        }
        fieldList.sort(new CategoryComparator());
        List<Pair<Field, FieldContextWrapper>> requiredFields =
                fieldList.stream().filter(p -> p.getValue().required()).collect(Collectors.toList());
        List<Pair<Field, FieldContextWrapper>> optionalFields =
                fieldList.stream().filter(p -> !p.getValue().required() && !p.getValue().deprecated())
                        .collect(Collectors.toList());
        List<Pair<Field, FieldContextWrapper>> deprecatedFields =
                fieldList.stream().filter(p -> p.getValue().deprecated()).collect(Collectors.toList());

        sb.append("# ").append(type).append("\n\n");
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
        List<Field> requiredFields = fieldList.stream().filter(isRequiredApiModel).collect(Collectors.toList());
        List<Field> optionalFields = fieldList.stream().filter(isOptionalApiModel).collect(Collectors.toList());

        sb.append("# ").append(type).append("\n\n");
        sb.append("## Required\n");
        writeDocListByApiModel(requiredFields, sb, obj);
        sb.append("## Optional\n");
        writeDocListByApiModel(optionalFields, sb, obj);

        return sb.toString();
    }
}
