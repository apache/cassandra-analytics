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

/**
 * A test startup validation that can succeed or fail as needed
 */
public class TestValidation implements StartupValidation
{
    private final boolean succeed;

    public static TestValidation failing()
    {
        return new TestValidation(false);
    }

    public static TestValidation succeeding()
    {
        return new TestValidation(true);
    }

    private TestValidation(boolean succeed)
    {
        this.succeed = succeed;
    }

    @Override
    public void validate()
    {
        if (!succeed)
        {
            throw new RuntimeException("Failing test validation");
        }
    }
}