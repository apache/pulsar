# Log4j

You can set the log level and configuration in the [log4j2.yaml](https://github.com/apache/pulsar/blob/d557e0aa286866363bc6261dec87790c055db1b0/conf/log4j2.yaml#L155) file. The following logging configuration parameters are available.

| Name                                                | Default                                     |
| --------------------------------------------------- | ------------------------------------------- |
| pulsar.root.logger                                  | WARN,CONSOLE                                |
| pulsar.log.dir                                      | logs                                        |
| pulsar.log.file                                     | pulsar.log                                  |
| log4j.rootLogger                                    | ${pulsar.root.logger}                       |
| log4j.appender.CONSOLE                              | org.apache.log4j.ConsoleAppender            |
| log4j.appender.CONSOLE.Threshold                    | DEBUG                                       |
| log4j.appender.CONSOLE.layout                       | org.apache.log4j.PatternLayout              |
| log4j.appender.CONSOLE.layout.ConversionPattern     | %d{ISO8601} - %-5p - [%t:%C{1}@%L] - %m%n   |
| log4j.appender.ROLLINGFILE                          | org.apache.log4j.DailyRollingFileAppender   |
| log4j.appender.ROLLINGFILE.Threshold                | DEBUG                                       |
| log4j.appender.ROLLINGFILE.File                     | ${pulsar.log.dir}/${pulsar.log.file}        |
| log4j.appender.ROLLINGFILE.layout                   | org.apache.log4j.PatternLayout              |
| log4j.appender.ROLLINGFILE.layout.ConversionPattern | %d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n     |
| log4j.appender.TRACEFILE                            | org.apache.log4j.FileAppender               |
| log4j.appender.TRACEFILE.Threshold                  | TRACE                                       |
| log4j.appender.TRACEFILE.File                       | pulsar-trace.log                            |
| log4j.appender.TRACEFILE.layout                     | org.apache.log4j.PatternLayout              |
| log4j.appender.TRACEFILE.layout.ConversionPattern   | %d{ISO8601} - %-5p [%t:%C{1}@%L][%x] - %m%n |

> Note: 'topic' in log4j2.appender is configurable.
>
> - If you want to append all logs to a single topic, set the same topic name.
> - If you want to append logs to different topics, you can set different topic names.

## Log4j shell

| Name                                                  | Default                          |
| ----------------------------------------------------- | -------------------------------- |
| bookkeeper.root.logger                                | ERROR,CONSOLE                    |
| log4j.rootLogger                                      | ${bookkeeper.root.logger}        |
| log4j.appender.CONSOLE                                | org.apache.log4j.ConsoleAppender |
| log4j.appender.CONSOLE.Threshold                      | DEBUG                            |
| log4j.appender.CONSOLE.layout                         | org.apache.log4j.PatternLayout   |
| log4j.appender.CONSOLE.layout.ConversionPattern       | %d{ABSOLUTE} %-5p %m%n           |
| log4j.logger.org.apache.zookeeper                     | ERROR                            |
| log4j.logger.org.apache.bookkeeper                    | ERROR                            |
| log4j.logger.org.apache.bookkeeper.bookie.BookieShell | INFO                             |
