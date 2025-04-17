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

package org.apache.cassandra.cdc;

import org.slf4j.Logger;

import org.apache.cassandra.cdc.msg.CdcEvent;

/**
 * Helper to produce structured log message for CDC events, etc.
 */
public interface CdcLogger
{
    /**
     * Create the log message with the input and log at the info level
     * @param logger logger to use to log
     * @param message message to log
     * @param event cdc event to log
     * @param topic message topic to log
     */
    void info(Logger logger, String message, CdcEvent event, String topic);

    /**
     * Create the log message with the input and log at the warn level
     * @param logger logger to use
     * @param message message to log
     * @param event cdc event to log
     * @param topic message topic to log
     * @param cause throwable to log
     */
    void warn(Logger logger, String message, CdcEvent event, String topic, Throwable cause);

    /**
     * Create the log message with the input and log at the error level
     * @param logger logger to use to log
     * @param message message to log
     * @param event cdc event to log
     * @param topic message topic to log
     * @param cause throwable to log
     */
    void error(Logger logger, String message, CdcEvent event, String topic, Throwable cause);
}
