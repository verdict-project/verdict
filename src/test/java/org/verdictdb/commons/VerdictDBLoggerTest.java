package org.verdictdb.commons;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/** Created by Dong Young Yoon on 7/25/18. */
public class VerdictDBLoggerTest {

  @Test
  public void loggerTest() {
    final VerdictDBLogger logger = VerdictDBLogger.getLogger(this.getClass());
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    VerdictOption options = new VerdictOption();
    options.setVerdictConsoleLogLevel("all");
    options.setVerdictFileLogLevel("all");
    appender.setName("testAppender");
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.TRACE);
    logger.debug("debug test");
    logger.error("error test");
    logger.info("info test");
    logger.trace("trace test");
    logger.warn("warn test");

    List<ILoggingEvent> events = appender.list;

    assertEquals(Level.DEBUG, events.get(0).getLevel());
    assertEquals("debug test", events.get(0).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(0).getLoggerName());

    assertEquals(Level.ERROR, events.get(1).getLevel());
    assertEquals("error test", events.get(1).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(1).getLoggerName());

    assertEquals(Level.INFO, events.get(2).getLevel());
    assertEquals("info test", events.get(2).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(2).getLoggerName());

    assertEquals(Level.TRACE, events.get(3).getLevel());
    assertEquals("trace test", events.get(3).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(3).getLoggerName());

    assertEquals(Level.WARN, events.get(4).getLevel());
    assertEquals("warn test", events.get(4).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(4).getLoggerName());

    appender.stop();
  }

  @Test
  public void loggerOptionTest() {
    final VerdictDBLogger logger = VerdictDBLogger.getLogger(this.getClass());
    VerdictOption options = new VerdictOption();
    options.setVerdictConsoleLogLevel("warn");
    options.setVerdictFileLogLevel("warn");
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setName("testAppender");
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.TRACE);
    logger.debug("debug test");
    logger.error("error test");
    logger.info("info test");
    logger.trace("trace test");
    logger.warn("warn test");

    List<ILoggingEvent> events = appender.list;

    assertEquals(Level.DEBUG, events.get(0).getLevel());
    assertEquals("debug test", events.get(0).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(0).getLoggerName());

    assertEquals(Level.ERROR, events.get(1).getLevel());
    assertEquals("error test", events.get(1).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(1).getLoggerName());

    assertEquals(Level.INFO, events.get(2).getLevel());
    assertEquals("info test", events.get(2).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(2).getLoggerName());

    assertEquals(Level.TRACE, events.get(3).getLevel());
    assertEquals("trace test", events.get(3).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(3).getLoggerName());

    assertEquals(Level.WARN, events.get(4).getLevel());
    assertEquals("warn test", events.get(4).getMessage());
    assertEquals("org.verdictdb.commons.VerdictDBLoggerTest", events.get(4).getLoggerName());

    appender.stop();
  }
}
