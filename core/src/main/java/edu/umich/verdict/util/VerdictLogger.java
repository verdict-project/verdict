package edu.umich.verdict.util;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class VerdictLogger {
	
	final static Logger logger = Logger.getLogger(VerdictLogger.class);
	
	public static void setLogLevel(String level) {
		if (level.equalsIgnoreCase("debug")) {
			logger.setLevel(Level.DEBUG);
		} else if (level.equalsIgnoreCase("info")) {
			logger.setLevel(Level.INFO);
		} else if (level.equalsIgnoreCase("warn")) {
			logger.setLevel(Level.WARN);
		} else if (level.equalsIgnoreCase("error")) {
			logger.setLevel(Level.ERROR);
		} else {
			logger.setLevel(Level.DEBUG);
		}
		VerdictLogger.info("Verdict's log level set to : " + level);
	}
	
	public static void info(Object msg) {
		logger.info(msg);
	}
	
	public static void debug(Object msg) {
		logger.debug(msg);
	}
	
	public static void error(Object msg) {
		logger.error(msg);
	}
	
	public static void warn(Object msg) {
		logger.warn(msg);
	}

	public static void info(Object caller, String msg) {
		info(String.format("[%s] %s", caller.getClass().getSimpleName(), msg));
	}

	public static void error(Object caller, String msg) {
		error(String.format("[%s] %s", caller.getClass().getSimpleName(), msg));
	}

	public static void infoPretty(Object caller, String msg, String forEveryLine) {
		String tokens[] = msg.split("\n");
		for (int i = 0; i < tokens.length; i++) {
			info(caller, forEveryLine + tokens[i]);
		}
	}

	public static void debug(Object caller, String msg) {
		debug(String.format("[%s] %s", caller.getClass().getSimpleName(), msg));
	}

	public static void warn(Object caller, String msg) {
		warn(String.format("[%s] %s", caller.getClass().getSimpleName(), msg));
	}

	public static void debugPretty(Object caller, String msg, String forEveryLine) {
		String tokens[] = msg.split("\n");
		for (int i = 0; i < tokens.length; i++) {
			debug(caller, forEveryLine + tokens[i]);
		}
	}

}
