---
id: functions-package-java
title: Package Java Functions
sidebar_label: "Package Java Functions"
---

There are two methods to package Java Functions, that is [uber JAR](#package-as-jar) and [NAR](#package-as-nar).

:::note

If you plan to package and distribute your function for others to use, you are obligated to
license and copyright your own code properly. Remember to add the license and copyright to
all libraries your code uses and to your distribution.

If you use the [NAR](#package-as-nar) method, the NAR plugin
automatically creates a `DEPENDENCIES` file in the generated NAR package, including the proper
licensing and copyrights of all libraries of your function.

For the runtime Java version, refer to [Pulsar Runtime Java Version Recommendation](https://github.com/apache/pulsar/blob/master/README.md#pulsar-runtime-java-version-recommendation) according to your target Pulsar version.

:::

## Package as JAR

To package a Java function as JAR, complete the following steps.

1. Create a new maven project with a pom file. In the following code sample, the value of `mainClass` is your package name.

   ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>

        <groupId>java-function</groupId>
        <artifactId>java-function</artifactId>
        <version>1.0-SNAPSHOT</version>

        <dependencies>
            <dependency>
                <groupId>org.apache.pulsar</groupId>
                <artifactId>pulsar-functions-api</artifactId>
                <version>2.10.0</version>
            </dependency>
        </dependencies>

        <build>
            <plugins>
                <plugin>
                    <artifactId>maven-assembly-plugin</artifactId>
                    <configuration>
                        <appendAssemblyId>false</appendAssemblyId>
                        <descriptorRefs>
                            <descriptorRef>jar-with-dependencies</descriptorRef>
                        </descriptorRefs>
                        <archive>
                        <manifest>
                            <mainClass>org.example.test.ExclamationFunction</mainClass>
                        </manifest>
                    </archive>
                    </configuration>
                    <executions>
                        <execution>
                            <id>make-assembly</id>
                            <phase>package</phase>
                            <goals>
                                <goal>assembly</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <configuration>
                        <release>17</release>
                    </configuration>
                </plugin>
            </plugins>
        </build>

    </project>
   ```

2. Package your Java function.

   ```bash
    mvn package
   ```

   After the Java function is packaged, a `target` directory is created automatically. Open the `target` directory to check if there is a JAR package similar to `java-function-1.0-SNAPSHOT.jar`.

3. Copy the packaged jar file to the Pulsar image.

   ```bash
    docker exec -it [CONTAINER ID] /bin/bash
    docker cp <path of java-function-1.0-SNAPSHOT.jar>  CONTAINER ID:/pulsar
   ```

4. Run the Java function using the following command.

   ```bash
    ./bin/pulsar-admin functions localrun \
       --classname org.example.test.ExclamationFunction \
       --jar java-function-1.0-SNAPSHOT.jar \
       --inputs persistent://public/default/my-topic-1 \
       --output persistent://public/default/test-1 \
       --tenant public \
       --namespace default \
       --name JavaFunction
   ```

   The following log indicates that the Java function starts successfully.

   ```text
    ...
    07:55:03.724 [main] INFO  org.apache.pulsar.functions.runtime.ProcessRuntime - Started process successfully
    ...
   ```

## Package as NAR

To package a Java function as NAR, complete the following steps.

1. Create a new maven project with a pom file.

   ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>

        <groupId>java-function</groupId>
        <artifactId>java-function</artifactId>
        <version>1.0-SNAPSHOT</version>

        <dependencies>
            <dependency>
                <groupId>org.apache.pulsar</groupId>
                <artifactId>pulsar-functions-api</artifactId>
                <version>@pulsar:version@</version>
            </dependency>
        </dependencies>

        <build>
            <plugins>
                <plugin>
                    <groupId>org.apache.nifi</groupId>
                    <artifactId>nifi-nar-maven-plugin</artifactId>
                    <version>1.2.0</version>
                    </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <configuration>
                        <release>17</release>
                    </configuration>
                </plugin>
            </plugins>
        </build>

    </project>
   ```

   You must also create a `resources/META-INF/services/pulsar-io.yaml` file. In the following code sample, the value of `functionClass` is your function class name. The `name` is the one used when the Function is deployed as a [built-in](functions-deploy-cluster-builtin.md) one.

   ```yaml
   name: java-function
   description: my java function
   functionClass: org.example.test.ExclamationFunction
   ```

2. Package your Java function.

   ```bash
   mvn package
   ```

   After the Java function is packaged, a `target` directory is created automatically. Open the `target` directory to check if there is a NAR package similar to `java-function-1.0-SNAPSHOT.nar`.

3. Copy the packaged NAR file to the Pulsar image.

   ```bash
    docker cp <path of java-function-1.0-SNAPSHOT.nar>  CONTAINER ID:/pulsar
   ```

4. Run the Java function using the following command.

   ```bash
    ./bin/pulsar-admin functions localrun \
       --jar java-function-1.0-SNAPSHOT.nar \
       --inputs persistent://public/default/my-topic-1 \
       --output persistent://public/default/test-1 \
       --tenant public \
       --namespace default \
       --name JavaFunction
   ```

   The following log indicates that the Java function starts successfully.

   ```text
    ...
    07:55:03.724 [main] INFO  org.apache.pulsar.functions.runtime.ProcessRuntime - Started process successfully
    ...
   ```
