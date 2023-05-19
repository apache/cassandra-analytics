/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.utils;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;

import org.jetbrains.annotations.Nullable;

/**
 * Helper class to automatically append fixed logging values for every message
 */
public final class LoggerHelper
{
    private final Logger logger;
    private final String keys;
    private final Object[] fixedArgumentss;

    public LoggerHelper(Logger logger, Object... fixedArguments)
    {
        this.logger = logger;
        this.keys = buildKey(fixedArguments);
        this.fixedArgumentss = extractArguments(fixedArguments);
    }

    private static String buildKey(Object... args)
    {
        Preconditions.checkArgument(args.length % 2 == 0, "Expect even number of key/value pairs in fixedArgs");
        return " " + IntStream.range(0, args.length / 2)
                              .map(index -> index * 2)
                              .mapToObj(index -> args[index])
                              .map(key -> key + "={}")
                              .collect(Collectors.joining(" "));
    }

    private static Object[] extractArguments(Object... arguments)
    {
        Preconditions.checkArgument(arguments.length % 2 == 0, "Expect even number of key/value pairs in fixedArgs");
        return IntStream.range(0, arguments.length / 2)
                        .map(index -> index * 2 + 1)
                        .mapToObj(index -> arguments[index])
                        .toArray(Object[]::new);
    }

    public void trace(String message, Object... arguments)
    {
        if (logger.isTraceEnabled())
        {
            logger.trace(logMsg(message, arguments), buildArguments(arguments));
        }
    }

    @SafeVarargs
    public final void trace(String message, Supplier<Object>... arguments)
    {
        if (logger.isTraceEnabled())
        {
            Object[] evaluatedArguments = Arrays.stream(arguments)
                                                .map(Supplier::get)
                                                .toArray();
            logger.trace(logMsg(message, evaluatedArguments), buildArguments(evaluatedArguments));
        }
    }

    public void debug(String message, Object... arguments)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(logMsg(message, arguments), buildArguments(arguments));
        }
    }

    @SafeVarargs
    public final void debug(String message, Supplier<Object>... arguments)
    {
        if (logger.isDebugEnabled())
        {
            Object[] evaluatedArguments = Arrays.stream(arguments)
                                                .map(Supplier::get)
                                                .toArray();
            logger.debug(logMsg(message, evaluatedArguments), buildArguments(evaluatedArguments));
        }
    }

    public void info(String message, Object... arguments)
    {
        logger.info(logMsg(message, arguments), buildArguments(arguments));
    }

    public void warn(String message, Throwable throwable, Object... arguments)
    {
        logger.warn(logMsg(message, arguments), buildArguments(throwable, arguments));
    }

    public void error(String message, Throwable throwable, Object... arguments)
    {
        logger.error(logMsg(message, arguments), buildArguments(throwable, arguments));
    }

    @VisibleForTesting
    String logMsg(String message, Object... arguments)
    {
        message += keys;
        if (0 < arguments.length)
        {
            message += buildKey(arguments);
        }
        return message;
    }

    private Object[] buildArguments(Object... arguments)
    {
        return buildArguments(null, arguments);
    }

    @VisibleForTesting
    Object[] buildArguments(@Nullable Throwable throwable, Object... arguments)
    {
        Object[] variableArguments = extractArguments(arguments);
        Object[] allArguments = new Object[variableArguments.length + fixedArgumentss.length + (throwable != null ? 1 : 0)];
        System.arraycopy(fixedArgumentss, 0, allArguments, 0, fixedArgumentss.length);
        System.arraycopy(variableArguments, 0, allArguments, fixedArgumentss.length, variableArguments.length);
        if (throwable != null)
        {
            allArguments[variableArguments.length + fixedArgumentss.length] = throwable;
        }
        return allArguments;
    }
}
