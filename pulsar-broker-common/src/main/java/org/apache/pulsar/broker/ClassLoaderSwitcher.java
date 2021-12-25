package org.apache.pulsar.broker;

/**
 * Help to switch the class loader of current thread to the NarClassLoader, and change it back when it's done.
 * With the help of try-with-resources statement, the code would be cleaner than using try finally every time.
 */
public class ClassLoaderSwitcher implements AutoCloseable {
    private final ClassLoader prevClassLoader;

    public ClassLoaderSwitcher(ClassLoader classLoader) {
        prevClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
    }

    @Override
    public void close() {
        Thread.currentThread().setContextClassLoader(prevClassLoader);
    }
}