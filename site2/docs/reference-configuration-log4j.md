# Log4j

You can set the log level and configuration in the [log4j2.yaml](https://github.com/apache/pulsar/blob/d557e0aa286866363bc6261dec87790c055db1b0/conf/log4j2.yaml#L155) file. The following logging configuration parameters are available.

### pulsar.root.logger

**Default**: WARN,CONSOLE

### pulsar.log.dir

**Default**: logs

### pulsar.log.file

**Default**: pulsar.log

### log4j.rootLogger

**Default**: ${pulsar.root.logger}

### log4j.appender.CONSOLE

**Default**: org.apache.log4j.ConsoleAppender

### log4j.appender.CONSOLE.Threshold

**Default**: DEBUG

### log4j.appender.CONSOLE.layout

**Default**: org.apache.log4j.PatternLayout

### log4j.appender.CONSOLE.layout.ConversionPattern

**Default**: %d{ISO8601} - %-5p - [%t:%C{1}@%L] - %m%n

### log4j.appender.ROLLINGFILE

**Default**: org.apache.log4j.DailyRollingFileAppender

### log4j.appender.ROLLINGFILE.Threshold

**Default**: DEBUG

### log4j.appender.ROLLINGFILE.File

**Default**: ${pulsar.log.dir}/${pulsar.log.file}

### log4j.appender.ROLLINGFILE.layout

**Default**: org.apache.log4j.PatternLayout

### log4j.appender.ROLLINGFILE.layout.ConversionPattern

**Default**: %d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n

### log4j.appender.TRACEFILE

**Default**: org.apache.log4j.FileAppender

### log4j.appender.TRACEFILE.Threshold

**Default**: TRACE

### log4j.appender.TRACEFILE.File

**Default**: pulsar-trace.log

### log4j.appender.TRACEFILE.layout

**Default**: org.apache.log4j.PatternLayout

### log4j.appender.TRACEFILE.layout.ConversionPattern

**Default**: %d{ISO8601} - %-5p [%t:%C{1}@%L][%x] - %m%n

> Note: 'topic' in log4j2.appender is configurable.
>
> - If you want to append all logs to a single topic, set the same topic name.
> - If you want to append logs to different topics, you can set different topic names.
