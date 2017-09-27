/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.util;

import java.lang.reflect.Method;

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
    
    private static String enclosingMethodName() {
        return Thread.currentThread().getStackTrace()[3].getMethodName();
//        Method method = caller.getClass().getEnclosingMethod();
//        if (method == null) {
//            return "static";
//        } else {
//            return method.getName();
//        }
    }

    public static void info(Object caller, String msg) {
        if (logger.getLevel().toString().equalsIgnoreCase("debug")) {
            info(String.format("[%s] %s",
                    caller.getClass().getSimpleName(),
                    msg));
        } else {
            info(String.format("%s", msg));
        }
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
        debug(String.format("[%s] %s",
                caller.getClass().getSimpleName(),
                msg));
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
