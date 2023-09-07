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

package org.apache.cassandra.spark.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FunctionalInterface
public interface StartupValidation
{
    Logger LOGGER = LoggerFactory.getLogger(StartupValidation.class);

    void validate();

    default void perform()
    {
        try
        {
            LOGGER.info("Performing startup validation with " + getClass());
            validate();
        }
        catch (Throwable throwable)
        {
            String message = "Failed startup validation with " + getClass();
            LOGGER.error(message, throwable);
            throw new RuntimeException(message, throwable);
        }
    }
}
