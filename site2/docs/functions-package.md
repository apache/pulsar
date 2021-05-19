---
id: functions-package
title: Package Pulsar Functions
sidebar_label: "How-to: Package"
---

You can package Pulsar functions in Java, Python, and Go. Packaging the window function in Java is the same as [packaging a function in Java](#java).

> **Note**    
> Currently, the window function is not available in Python and Go.

## Prerequisite

Before running a Pulsar function, you need to start Pulsar. You can [run a standalone Pulsar in Docker](getting-started-docker.md), or [run Pulsar in Kubernetes](getting-started-helm.md).

To check whether the Docker image starts, you can use the `docker ps` command.

## Java 

To package a function in Java, complete the following steps.

1. Create a new maven project with a pom file. In the following code sample, the value of `mainClass` is your package name.

    ```Java
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
                <version>2.6.0</version>
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
                        <source>8</source>
                        <target>8</target>
                    </configuration>
                </plugin>
            </plugins>
        </build>

    </project>
    ```

2. Write a Java function.

    ```
    package org.example.test;

    import java.util.function.Function;

    public class ExclamationFunction implements Function<String, String> {
        @Override
        public String apply(String s) {
            return "This is my function!";
        }
    }
    ```
 
     For the imported package, you can use one of the following interfaces:
    - Function interface provided by Java 8: `java.util.function.Function`
    - Pulsar Function interface: `org.apache.pulsar.functions.api.Function`

    The main difference between the two interfaces is that the `org.apache.pulsar.functions.api.Function` interface provides the context interface. When you write a function and want to interact with it, you can use context to obtain a wide variety of information and functionality for Pulsar Functions.

    The following example uses `org.apache.pulsar.functions.api.Function` interface with context.

    ```
    package org.example.functions;
    import org.apache.pulsar.functions.api.Context;
    import org.apache.pulsar.functions.api.Function;
    
    import java.util.Arrays;
    public class WordCountFunction implements Function<String, Void> {
       // This function is invoked every time a message is published to the input topic
        @Override
        public Void process(String input, Context context) throws Exception {
           Arrays.asList(input.split(" ")).forEach(word -> {
               String counterKey = word.toLowerCase();
               context.incrCounter(counterKey, 1);
            });
           return null;
       }
     }
     ```

3. Package the Java function.

    ```bash
    mvn package
    ```

    After the Java function is packaged, a `target` directory is created automatically. Open the `target` directory to check if there is a JAR package similar to `java-function-1.0-SNAPSHOT.jar`.


4.  Run the Java function.

     (1) Copy the packaged jar file to the Pulsar image.

    ```bash
    docker exec -it [CONTAINER ID] /bin/bash
    docker cp <path of java-function-1.0-SNAPSHOT.jar>  CONTAINER ID:/pulsar
    ```

    (2) Run the Java function using the following command.

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

## Python 

Python Function supports the following three formats:

- One python file
- ZIP file
- PIP

### One python file

To package a function with **one python file** in Python, complete the following steps.

1. Write a Python function.

    ```
    from pulsar import Function //  import the Function module from Pulsar

    # The classic ExclamationFunction that appends an exclamation at the end
    # of the input
    class ExclamationFunction(Function):
      def __init__(self):
        pass

      def process(self, input, context):
        return input + '!'
    ```

    In this example, when you write a Python function, you need to inherit the Function class and implement the `process()` method.

    `process()` mainly has two parameters: 

    - `input` represents your input.
  
    - `context` represents an interface exposed by the Pulsar Function. You can get the attributes in the Python function based on the provided context object.

2. Install a Python client.

    The implementation of a Python function depends on the Python client, so before deploying a Python function, you need to install the corresponding version of the Python client. 

    ```bash
    pip install python-client==2.6.0
    ```

3. Run the Python Function.

    (1) Copy the Python function file to the Pulsar image.

    ```bash
    docker exec -it [CONTAINER ID] /bin/bash
    docker cp <path of Python function file>  CONTAINER ID:/pulsar
    ```

    (2) Run the Python function using the following command.

    ```bash
    ./bin/pulsar-admin functions localrun \
    --classname org.example.test.ExclamationFunction \
    --py <path of Python Function file> \
    --inputs persistent://public/default/my-topic-1 \
    --output persistent://public/default/test-1 \
    --tenant public \
    --namespace default \
    --name PythonFunction
    ```

    The following log indicates that the Python function starts successfully.

    ```text
    ...
    07:55:03.724 [main] INFO  org.apache.pulsar.functions.runtime.ProcessRuntime - Started process successfully
    ...
    ```

### ZIP file

To package a function with the **ZIP file** in Python, complete the following steps.

1. Prepare the ZIP file.

    The following is required when packaging the ZIP file of the Python Function.

    ```text
    Assuming the zip file is named as `func.zip`, unzip the `func.zip` folder:
        "func/src"
        "func/requirements.txt"
        "func/deps"
    ```
    Take [exclamation.zip](https://github.com/apache/pulsar/tree/master/tests/docker-images/latest-version-image/python-examples) as an example. The internal structure of the example is as follows.

    ```text
    .
    ├── deps
    │   └── sh-1.12.14-py2.py3-none-any.whl
    └── src
        └── exclamation.py
    ```

2. Run the Python Function.

    (1) Copy the ZIP file to the Pulsar image.

    ```bash
    docker exec -it [CONTAINER ID] /bin/bash
    docker cp <path of ZIP file>  CONTAINER ID:/pulsar
    ```

    (2) Run the Python function using the following command.

    ```bash
    ./bin/pulsar-admin functions localrun \
    --classname exclamation \
    --py <path of ZIP file> \
    --inputs persistent://public/default/in-topic \
    --output persistent://public/default/out-topic \
    --tenant public \
    --namespace default \
    --name PythonFunction
    ```

    The following log indicates that the Python function starts successfully.

    ```text
    ...
    07:55:03.724 [main] INFO  org.apache.pulsar.functions.runtime.ProcessRuntime - Started process successfully
    ...
    ```

### PIP

The PIP method is only supported in Kubernetes runtime. To package a function with **PIP** in Python, complete the following steps.

1. Configure the `functions_worker.yml` file.

    ```text
    #### Kubernetes Runtime ####
    installUserCodeDependencies: true
    ```

2. Write your Python Function.

    ```
    from pulsar import Function
    import js2xml

    # The classic ExclamationFunction that appends an exclamation at the end
    # of the input
    class ExclamationFunction(Function):
     def __init__(self):
       pass

     def process(self, input, context):
      // add your logic
      return input + '!'
    ```

    You can introduce additional dependencies. When Python Function detects that the file currently used is `whl` and the `installUserCodeDependencies` parameter is specified, the system uses the `pip install` command to install the dependencies required in Python Function.

3. Generate the `whl` file.

    ```shell script
    $ cd $PULSAR_HOME/pulsar-functions/scripts/python
    $ chmod +x generate.sh
    $ ./generate.sh <path of your Python Function> <path of the whl output dir> <the version of whl>
    # e.g: ./generate.sh /path/to/python /path/to/python/output 1.0.0
    ```

    The output is written in `/path/to/python/output`:

    ```text
    -rw-r--r--  1 root  staff   1.8K  8 27 14:29 pulsarfunction-1.0.0-py2-none-any.whl
    -rw-r--r--  1 root  staff   1.4K  8 27 14:29 pulsarfunction-1.0.0.tar.gz
    -rw-r--r--  1 root  staff     0B  8 27 14:29 pulsarfunction.whl
    ```

## Go 

To package a function in Go, complete the following steps.

1. Write a Go function.

    Currently, Go function can be **only** implemented using SDK and the interface of the function is exposed in the form of SDK. Before using the Go function, you need to import "github.com/apache/pulsar/pulsar-function-go/pf". 

    ```
    import (
        "context"
        "fmt"

        "github.com/apache/pulsar/pulsar-function-go/pf"
    )

    func HandleRequest(ctx context.Context, input []byte) error {
        fmt.Println(string(input) + "!")
        return nil
    }

    func main() {
        pf.Start(HandleRequest)
    }
    ```

    You can use context to connect to the Go function.
    
    ```
    if fc, ok := pf.FromContext(ctx); ok {
        fmt.Printf("function ID is:%s, ", fc.GetFuncID())
        fmt.Printf("function version is:%s\n", fc.GetFuncVersion())
     }
     ```

    When writing a Go function, remember that
    - In `main()`, you **only** need to register the function name to `Start()`. **Only** one function name is received in `Start()`. 
    - Go function uses Go reflection, which is based on the received function name, to verify whether the parameter list and returned value list are correct. The parameter list and returned value list **must be** one of the following sample functions:
    
      ```
       func ()
       func () error
       func (input) error
       func () (output, error)
       func (input) (output, error)
       func (context.Context) error
       func (context.Context, input) error
       func (context.Context) (output, error)
       func (context.Context, input) (output, error)
       ```

2. Build the Go function.

    ```
    go build <your Go Function filename>.go 
    ```

3. Run the Go Function.

    (1) Copy the Go function file to the Pulsar image.

    ```bash
    docker exec -it [CONTAINER ID] /bin/bash
    docker cp <your go function path>  CONTAINER ID:/pulsar
    ```

    (2) Run the Go function with the following command.

    ```
    ./bin/pulsar-admin functions localrun \
        --go [your go function path] 
        --inputs [input topics] \
        --output [output topic] \
        --tenant [default:public] \
        --namespace [default:default] \
        --name [custom unique go function name] 
    ```

    The following log indicates that the Go function starts successfully.

    ```text
    ...
    07:55:03.724 [main] INFO  org.apache.pulsar.functions.runtime.ProcessRuntime - Started process successfully
    ...
    ```

## Start Functions in cluster mode
If you want to start a function in cluster mode, replace `localrun` with `create` in the commands above. The following log indicates that your function starts successfully.

 ```text
   "Created successfully"
   ```

For information about parameters on `--classname`, `--jar`, `--py`, `--go`, `--inputs`, run the command `./bin/pulsar-admin functions` or see [here](reference-pulsar-admin.md#functions).