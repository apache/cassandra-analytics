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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A singleton class for performing the startup validation
 * using a list of necessary {@link StartupValidation} instances
 */
public final class StartupValidator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StartupValidator.class);
    private static final ThreadLocal<StartupValidator> INSTANCE = ThreadLocal.withInitial(StartupValidator::new);
    private static final String DISABLE = "SKIP_STARTUP_VALIDATIONS";

    private final List<StartupValidation> validations = new CopyOnWriteArrayList<>();

    private StartupValidator()
    {
    }

    public static StartupValidator instance()
    {
        return INSTANCE.get();
    }

    public void register(StartupValidation validation)
    {
        validations.add(validation);
    }

    @VisibleForTesting
    void reset()
    {
        validations.clear();
    }

    public void perform()
    {
        StringBuilder message = new StringBuilder(1024);
        boolean passed = true;

        if (enabled())
        {
            message.append("Performed startup validations:");
            for (StartupValidation validation : validations)
            {
                String name = validation.getClass().getCanonicalName();
                message.append(System.lineSeparator() + " * " + name + ": ");
                Throwable result = validation.perform();
                if (result == null)
                {
                    LOGGER.debug("Passed startup validation with " + name);
                    message.append("PASSED");
                }
                else
                {
                    LOGGER.error("Failed startup validation with " + name, result);
                    message.append("FAILED");
                    passed = false;
                }
            }
        }
        else
        {
            message.append("Skipped startup validations");
        }

        // Regardless of the enabled status, remove all validations, because the list just keeps growing otherwise
        reset();

        if (passed)
        {
            LOGGER.info(message.toString());
        }
        else
        {
            LOGGER.error(message.toString());
            throw new RuntimeException("Failed some of startup validations");
        }
    }

    public boolean enabled()
    {
        return "false".equalsIgnoreCase(System.getProperty(DISABLE));
    }
}
