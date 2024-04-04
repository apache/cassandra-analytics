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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests that cover basic functioning of the startup validation logic
 */
public class StartupValidatorTests
{
    @BeforeEach
    public void beforeEach()
    {
        StartupValidator.instance().reset();
    }

    @Test
    public void testWithoutValidations()
    {
        assertDoesNotThrow(StartupValidator.instance()::perform);
    }

    @Test
    public void testSucceedingValidations()
    {
        StartupValidator.instance().register(TestValidation.succeeding());
        StartupValidator.instance().register(TestValidation.succeeding());
        StartupValidator.instance().register(TestValidation.succeeding());

        assertDoesNotThrow(StartupValidator.instance()::perform);
    }

    @Test
    public void testFailingValidations()
    {
        StartupValidator.instance().register(TestValidation.succeeding());
        StartupValidator.instance().register(TestValidation.failing());
        StartupValidator.instance().register(TestValidation.succeeding());

        RuntimeException exception = assertThrows(RuntimeException.class, StartupValidator.instance()::perform);
        assertEquals("Failed some of startup validations", exception.getMessage());
    }

    @Test
    public void testSkipValidations()
    {
        System.setProperty("SKIP_STARTUP_VALIDATIONS", "true");
        StartupValidator.instance().register(TestValidation.succeeding());
        StartupValidator.instance().register(TestValidation.failing());
        StartupValidator.instance().register(TestValidation.succeeding());

        StartupValidator.instance().perform();
        System.clearProperty("SKIP_STARTUP_VALIDATIONS");
    }

    @AfterAll
    public static void afterAll()
    {
        StartupValidator.instance().reset();
    }
}
