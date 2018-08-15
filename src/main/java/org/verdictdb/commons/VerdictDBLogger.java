/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.commons;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Iterator;

public class VerdictDBLogger implements org.slf4j.Logger {

  private org.slf4j.Logger logger;

  private static final String VERDICT_LOGGER_NAME = "org.verdictdb";

  private VerdictDBLogger(org.slf4j.Logger logger) {
    this.logger = logger;
  }

  public static VerdictDBLogger getLogger(Class<?> c) {
    return new VerdictDBLogger(LoggerFactory.getLogger(c));
  }

  public static VerdictDBLogger getLogger(String name) {
    return new VerdictDBLogger(LoggerFactory.getLogger(name));
  }

  public static void setConsoleLogLevel(String level) {
    ThresholdFilter thresholdFilter = new ThresholdFilter();
    thresholdFilter.setLevel(level);
    Logger root = (Logger) LoggerFactory.getLogger(VERDICT_LOGGER_NAME);
    Iterator<Appender<ILoggingEvent>> iterator = root.iteratorForAppenders();
    while (iterator.hasNext()) {
      Appender<ILoggingEvent> appender = iterator.next();
      if (appender instanceof ConsoleAppender) {
        ConsoleAppender ca = (ConsoleAppender) appender;
        ca.clearAllFilters();
        ca.addFilter(thresholdFilter);
        thresholdFilter.start();
      }
    }
  }

  public static void setFileLogLevel(String level) {
    ThresholdFilter thresholdFilter = new ThresholdFilter();
    thresholdFilter.setLevel(level);
    // For file log, we need to change root logger's appender.
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    Iterator<Appender<ILoggingEvent>> iterator = root.iteratorForAppenders();
    while (iterator.hasNext()) {
      Appender<ILoggingEvent> appender = iterator.next();
      if (appender instanceof FileAppender) {
        FileAppender fa = (FileAppender) appender;
        fa.clearAllFilters();
        fa.addFilter(thresholdFilter);
        thresholdFilter.start();
      }
    }
  }

  public void setLevel(Level level) {
    if (logger instanceof ch.qos.logback.classic.Logger) {
      ((ch.qos.logback.classic.Logger) logger).setLevel(level);
    }
  }

  public void SetRootLogLevel(Level level) {
    ((Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME)).setLevel(level);
  }

  public void addAppender(Appender<ILoggingEvent> appender) {
    if (logger instanceof ch.qos.logback.classic.Logger) {
      appender.setContext(((ch.qos.logback.classic.Logger) logger).getLoggerContext());
      ((ch.qos.logback.classic.Logger) logger).addAppender(appender);
    }
  }

  @Override
  public String getName() {
    return logger.getName();
  }

  @Override
  public boolean isTraceEnabled() {
    return logger.isTraceEnabled();
  }

  @Override
  public void trace(String s) {
    logger.trace(s);
  }

  @Override
  public void trace(String s, Object o) {
    logger.trace(s, o);
  }

  @Override
  public void trace(String s, Object o, Object o1) {
    logger.trace(s, o, o1);
  }

  @Override
  public void trace(String s, Object... objects) {
    logger.trace(s, objects);
  }

  @Override
  public void trace(String s, Throwable throwable) {
    logger.trace(s, throwable);
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return logger.isTraceEnabled(marker);
  }

  @Override
  public void trace(Marker marker, String s) {
    logger.trace(marker, s);
  }

  @Override
  public void trace(Marker marker, String s, Object o) {
    logger.trace(marker, s, o);
  }

  @Override
  public void trace(Marker marker, String s, Object o, Object o1) {
    logger.trace(marker, s, o, o1);
  }

  @Override
  public void trace(Marker marker, String s, Object... objects) {
    logger.trace(marker, s, objects);
  }

  @Override
  public void trace(Marker marker, String s, Throwable throwable) {
    logger.trace(marker, s, throwable);
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  @Override
  public void debug(String s) {
    synchronized (VerdictDBLogger.class) {
      logger.debug(s);
    }
  }

  @Override
  public void debug(String s, Object o) {
    logger.debug(s, o);
  }

  @Override
  public void debug(String s, Object o, Object o1) {
    logger.debug(s, o, o1);
  }

  @Override
  public void debug(String s, Object... objects) {
    logger.debug(s, objects);
  }

  @Override
  public void debug(String s, Throwable throwable) {
    logger.debug(s, throwable);
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return logger.isDebugEnabled(marker);
  }

  @Override
  public void debug(Marker marker, String s) {
    logger.debug(marker, s);
  }

  @Override
  public void debug(Marker marker, String s, Object o) {
    logger.debug(marker, s, o);
  }

  @Override
  public void debug(Marker marker, String s, Object o, Object o1) {
    logger.debug(marker, s, o, o1);
  }

  @Override
  public void debug(Marker marker, String s, Object... objects) {
    logger.debug(marker, s, objects);
  }

  @Override
  public void debug(Marker marker, String s, Throwable throwable) {
    logger.debug(marker, s, throwable);
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public void info(String s) {
    logger.info(s);
  }

  @Override
  public void info(String s, Object o) {
    logger.info(s, o);
  }

  @Override
  public void info(String s, Object o, Object o1) {
    logger.info(s, o, o1);
  }

  @Override
  public void info(String s, Object... objects) {
    logger.info(s, objects);
  }

  @Override
  public void info(String s, Throwable throwable) {
    logger.info(s, throwable);
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return logger.isInfoEnabled(marker);
  }

  @Override
  public void info(Marker marker, String s) {
    logger.info(marker, s);
  }

  @Override
  public void info(Marker marker, String s, Object o) {
    logger.info(marker, s, o);
  }

  @Override
  public void info(Marker marker, String s, Object o, Object o1) {
    logger.info(marker, s, o, o1);
  }

  @Override
  public void info(Marker marker, String s, Object... objects) {
    logger.info(marker, s, objects);
  }

  @Override
  public void info(Marker marker, String s, Throwable throwable) {
    logger.info(marker, s, throwable);
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }

  @Override
  public void warn(String s) {
    logger.warn(s);
  }

  @Override
  public void warn(String s, Object o) {
    logger.warn(s, o);
  }

  @Override
  public void warn(String s, Object... objects) {
    logger.warn(s, objects);
  }

  @Override
  public void warn(String s, Object o, Object o1) {
    logger.warn(s, o, o1);
  }

  @Override
  public void warn(String s, Throwable throwable) {
    logger.warn(s, throwable);
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return logger.isWarnEnabled();
  }

  @Override
  public void warn(Marker marker, String s) {
    logger.warn(marker, s);
  }

  @Override
  public void warn(Marker marker, String s, Object o) {
    logger.warn(marker, s, o);
  }

  @Override
  public void warn(Marker marker, String s, Object o, Object o1) {
    logger.warn(marker, s, o, o1);
  }

  @Override
  public void warn(Marker marker, String s, Object... objects) {
    logger.warn(marker, s, objects);
  }

  @Override
  public void warn(Marker marker, String s, Throwable throwable) {
    logger.warn(marker, s, throwable);
  }

  @Override
  public boolean isErrorEnabled() {
    return logger.isErrorEnabled();
  }

  @Override
  public void error(String s) {
    logger.error(s);
  }

  @Override
  public void error(String s, Object o) {
    logger.error(s, o);
  }

  @Override
  public void error(String s, Object o, Object o1) {
    logger.error(s, o, o1);
  }

  @Override
  public void error(String s, Object... objects) {
    logger.error(s, objects);
  }

  @Override
  public void error(String s, Throwable throwable) {
    logger.error(s, throwable);
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return logger.isErrorEnabled(marker);
  }

  @Override
  public void error(Marker marker, String s) {
    logger.error(marker, s);
  }

  @Override
  public void error(Marker marker, String s, Object o) {
    logger.error(marker, s, o);
  }

  @Override
  public void error(Marker marker, String s, Object o, Object o1) {
    logger.error(marker, s, o, o1);
  }

  @Override
  public void error(Marker marker, String s, Object... objects) {
    logger.error(marker, s, objects);
  }

  @Override
  public void error(Marker marker, String s, Throwable throwable) {
    logger.error(marker, s, throwable);
  }
}
