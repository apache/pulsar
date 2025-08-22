# PIP-86: Pulsar Functions: Preload and release external resources

It is very useful in many scenarios to provide safe and convenient capabilities for function's external resource initialization and release. In addition to the normal data processing path, it enables functions to use HookFunction to manage external resources

At present, in order to process data, only the logic of resource initialization -> processing -> release and shutdown can be written in the process() of Function. This method is complicated, insecure, and unnecessary.

Instead, we should have a new standard way for users to use Function easily and safely. Summarized as follows:

Before Function starts, some resources only need to be initialized once, and there is no need to make various judgments in the process() method of the Function interface

After closing the Function, in the process of using process(), you need to manually close the referenced external resources, which need to be released separately in the close() of javaInstance 

## API and Implementation Changes

The organization of the function implementation hierarchy has been added, it currently looks like the following figure:

Use Cases:

Before transformation

```
public class DemoFunction implements Function<String, String>{
    RedisClient client;
    @Override
    public String process(String str, Context context) {
        1.client=init();
        2.Object object = client.get(key);
        //Data enrichment
        3.client.close();
        return null;
    }
}
```

After the transformation

```
public class DemoFunction implements HookFunction<String, String>{
    RedisClient client;
    @Override
    public void setup(Context context) {
        Map<String, Object> connectInfo = context.getUserConfigMap();
        client=init();
    }
 
    @Override
    public  String process(String str, Context context) {
        Object object = client.get(key);
        //Data enrichment
        return null;
    }
 
    @Override
    public void cleanup() {
        client.close();
    }
}
```
It is quite simple and clear to use in function processing code.
