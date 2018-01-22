package org.apache.pulsar.functions.api.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.dsl.windowing.Window;
import org.apache.pulsar.functions.api.dsl.windowing.WindowContext;
import org.apache.pulsar.functions.api.dsl.windowing.WindowedPulsarFunction;

@Slf4j
public class WindowedFunction extends WindowedPulsarFunction<String, String> {
    @Override
    public String handleRequest(Window<String> inputWindow, WindowContext context) throws Exception {

        String sum = "";
        for (String entry : inputWindow.get()) {
            sum += entry;
        }
        log.info("Start: {} End: {} Sum: {}", inputWindow.getStartTimestamp(), inputWindow.getEndTimestamp(), sum);
        return sum;
    }
}
