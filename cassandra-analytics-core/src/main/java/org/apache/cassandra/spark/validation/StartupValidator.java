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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A singleton class for performing the startup validation
 * using a list of necessary {@link StartupValidation} instances
 */
public class StartupValidator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StartupValidator.class);
    private static final StartupValidator INSTANCE = new StartupValidator();

    private final List<StartupValidation> validations = new ArrayList<>();

    private StartupValidator()
    {
    }

    public void register(StartupValidation validation)
    {
        validations.add(validation);
    }

    public void perform()
    {
        LOGGER.info("Performing startup validations");
        validations.forEach(StartupValidation::perform);
        LOGGER.info("Completed startup validations");
    }

    public static StartupValidator instance()
    {
        return INSTANCE;
    }
}
