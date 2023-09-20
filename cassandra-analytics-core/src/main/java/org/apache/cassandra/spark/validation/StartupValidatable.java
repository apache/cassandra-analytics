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
 * An interface for a class that requires and can perform startup validation using {@link StartupValidator}
 */
public interface StartupValidatable
{
    /**
     * Performs startup validation using {@link StartupValidator} with currently registered {@link StartupValidation}s,
     * throws a {@link RuntimeException} if any violations are found,
     * needs to be invoked once per execution before any actual work is started
     */
    void startupValidate();
}
