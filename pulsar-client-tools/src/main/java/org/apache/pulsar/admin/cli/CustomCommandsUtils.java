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
package org.apache.pulsar.admin.cli;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtNewConstructor;
import javassist.Modifier;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import javassist.bytecode.annotation.ArrayMemberValue;
import javassist.bytecode.annotation.BooleanMemberValue;
import javassist.bytecode.annotation.MemberValue;
import javassist.bytecode.annotation.StringMemberValue;
import lombok.Setter;
import org.apache.pulsar.admin.cli.extensions.CommandExecutionContext;
import org.apache.pulsar.admin.cli.extensions.CustomCommand;
import org.apache.pulsar.admin.cli.extensions.CustomCommandGroup;
import org.apache.pulsar.admin.cli.extensions.ParameterDescriptor;
import org.apache.pulsar.admin.cli.extensions.ParameterType;
import org.apache.pulsar.client.admin.PulsarAdmin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public final class CustomCommandsUtils {
    private CustomCommandsUtils() {
    }

    public static Object generateCliCommand(CustomCommandGroup group, CommandExecutionContext context,
                                            Supplier<PulsarAdmin> pulsarAdmin){
        List<CustomCommand> commands = group.commands(context);
        String description = group.description();

        try {
            ClassPool pool = ClassPool.getDefault();
            CtClass ctClass = pool.makeClass("CustomCommandGroup" + group
                    + "_" + System.nanoTime());
            ctClass.setSuperclass(pool.get(CmdBaseAdapter.class.getName()));

            // add class annotation
            ClassFile classFile = ctClass.getClassFile();
            ConstPool constpool = classFile.getConstPool();
            AnnotationsAttribute annotationsAttribute = new AnnotationsAttribute(constpool,
                    AnnotationsAttribute.visibleTag);
            Annotation annotation = new Annotation(Command.class.getName(), constpool);
            ArrayMemberValue descArrayMemberValue = new ArrayMemberValue(classFile.getConstPool());
            descArrayMemberValue.setValue(
                    new MemberValue[]{new StringMemberValue(description, classFile.getConstPool())});
            annotation.addMemberValue("description", descArrayMemberValue);
            annotation.addMemberValue("name", new StringMemberValue(group.name(),
                    classFile.getConstPool()));
            annotationsAttribute.setAnnotation(annotation);
            ctClass.getClassFile().addAttribute(annotationsAttribute);

            // Add a constructor which calls super( ... );
            CtClass[] params = new CtClass[]{
                    pool.get(String.class.getName()),
                    pool.get(Supplier.class.getName()),
                    pool.get(List.class.getName()),
                    pool.get(CommandExecutionContext.class.getName())
            };
            final CtConstructor ctor = CtNewConstructor.make(params, null, CtNewConstructor.PASS_PARAMS,
                    null, null, ctClass);
            ctClass.addConstructor(ctor);

            return ctClass.toClass().getConstructor(String.class, Supplier.class, List.class,
                            CommandExecutionContext.class)
                    .newInstance(group.name(), pulsarAdmin, commands, context);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static class CmdBaseAdapter extends CmdBase {
        public CmdBaseAdapter(String cmdName, Supplier<PulsarAdmin> adminSupplier,
                              List<CustomCommand> customCommands, CommandExecutionContext context) {
            super(cmdName, adminSupplier);
            for (CustomCommand command : customCommands) {
                String name = command.name();
                DecoratedCommand commandImpl = generateCustomCommand(cmdName, name, command);
                commandImpl.setCommand(command);
                commandImpl.setContext(context);
                addCommand(name, commandImpl);
            }
        }
    }


    @Setter
    public static class DecoratedCommand extends CliCommand {

        private CustomCommand command;
        private CommandExecutionContext context;

        public DecoratedCommand() {
        }

        @Override
        public void run() throws Exception {
            Map<String, Object> parameters = new HashMap<>();
            for (Field f : this.getClass().getFields()) {
                parameters.put(f.getName(), f.get(this));
            }
            command.execute(parameters, context);
        }
    }

    private static DecoratedCommand generateCustomCommand(String group, String name, CustomCommand command) {
        try {
            String description = command.description();
            ClassPool pool = ClassPool.getDefault();
            CtClass ctClass = pool.makeClass("CustomCommand" + group
                    + "_" + name + "_" + System.nanoTime());
            ctClass.setSuperclass(pool.get(DecoratedCommand.class.getName()));

            // add class annotation

            ClassFile classFile = ctClass.getClassFile();
            ConstPool constpool = classFile.getConstPool();

            AnnotationsAttribute annotationsAttribute = new AnnotationsAttribute(constpool,
                    AnnotationsAttribute.visibleTag);
            Annotation annotation = new Annotation(Command.class.getName(), constpool);
            ArrayMemberValue descArrayMemberValue = new ArrayMemberValue(classFile.getConstPool());
            descArrayMemberValue.setValue(
                    new MemberValue[]{new StringMemberValue(description, classFile.getConstPool())});
            annotation.addMemberValue("description", descArrayMemberValue);
            annotation.addMemberValue("name", new StringMemberValue(name, classFile.getConstPool()));
            annotationsAttribute.setAnnotation(annotation);
            ctClass.getClassFile().addAttribute(annotationsAttribute);


            // add fields
            List<ParameterDescriptor> parameters = command.parameters();
            for (ParameterDescriptor parameterDescriptor : parameters) {
                CtClass fieldType;
                switch (parameterDescriptor.getType()) {
                    case BOOLEAN_FLAG:
                        //  command -parameter
                        fieldType = CtClass.booleanType;
                        break;
                    case BOOLEAN:
                        // command -parameter true|false
                        fieldType = pool.get(Boolean.class.getName());
                        break;
                    case INTEGER:
                        // command -parameter 123
                        fieldType = CtClass.intType;
                        break;
                    case STRING:
                        // command -parameter foo
                        fieldType = pool.get(String.class.getName());
                        break;
                    default:
                        throw new IllegalStateException();
                }
                List<String> parameterNames = parameterDescriptor.getNames();
                if (parameterNames == null || parameterNames.isEmpty()) {
                    // ignore
                    continue;
                }
                String fieldName = parameterNames.get(0);
                CtField field = new CtField(fieldType, fieldName, ctClass);

                AnnotationsAttribute fieldAnnotationsAttribute = new AnnotationsAttribute(constpool,
                        AnnotationsAttribute.visibleTag);
                Annotation fieldAnnotation;
                if (!parameterDescriptor.isMainParameter()) {
                    fieldAnnotation = new Annotation(Option.class.getName(), constpool);
                    MemberValue[] memberValues = new MemberValue[parameterNames.size()];
                    int i = 0;
                    for (String parameterName : parameterNames) {
                        memberValues[i++] = new StringMemberValue(parameterName, classFile.getConstPool());
                    }
                    ArrayMemberValue arrayMemberValue = new ArrayMemberValue(classFile.getConstPool());
                    arrayMemberValue.setValue(memberValues);
                    fieldAnnotation.addMemberValue("names", arrayMemberValue);
                    fieldAnnotation.addMemberValue("required",
                            new BooleanMemberValue(parameterDescriptor.isRequired(), classFile.getConstPool()));
                    if (parameterDescriptor.getType() == ParameterType.BOOLEAN) {
                        fieldAnnotation.addMemberValue("arity",
                                new StringMemberValue("1", classFile.getConstPool()));
                    }
                } else {
                    fieldAnnotation = new Annotation(Parameters.class.getName(), constpool);
                    String arityValue = parameterDescriptor.isRequired() ? "1" : "0..1";
                    fieldAnnotation.addMemberValue("arity",
                            new StringMemberValue(arityValue, classFile.getConstPool()));
                }
                ArrayMemberValue optionDescArrayMemberValue = new ArrayMemberValue(classFile.getConstPool());
                optionDescArrayMemberValue.setValue(
                        new MemberValue[]{
                                new StringMemberValue(parameterDescriptor.getDescription(), classFile.getConstPool())});
                fieldAnnotation.addMemberValue("description", optionDescArrayMemberValue);
                fieldAnnotationsAttribute.setAnnotation(fieldAnnotation);
                field.getFieldInfo().addAttribute(fieldAnnotationsAttribute);
                field.setModifiers(Modifier.PUBLIC);

                ctClass.addField(field);
            }


            return (DecoratedCommand) ctClass.toClass().getConstructor().newInstance();
        } catch (Throwable t) {
            t.printStackTrace(System.out);
            throw new RuntimeException(t);
        }
    }
}
