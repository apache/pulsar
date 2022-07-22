# Log4j shell

### bookkeeper.root.logger

**Default**: ERROR,CONSOLE

### log4j.rootLogger

**Default**: ${bookkeeper.root.logger}

### log4j.appender.CONSOLE

**Default**: org.apache.log4j.ConsoleAppender

### log4j.appender.CONSOLE.Threshold

**Default**: DEBUG

### log4j.appender.CONSOLE.layout

**Default**: org.apache.log4j.PatternLayout

### log4j.appender.CONSOLE.layout.ConversionPattern

**Default**: %d{ABSOLUTE} %-5p %m%n

### log4j.logger.org.apache.zookeeper

**Default**: ERROR

### log4j.logger.org.apache.bookkeeper

**Default**: ERROR

### log4j.logger.org.apache.bookkeeper.bookie.BookieShell

**Default**: INFO
