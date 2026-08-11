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

package org.apache.cassandra.spark;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.junit.jupiter.api.extension.DynamicTestInvocationContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import org.apache.cassandra.bridge.CassandraVersion;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class SSTableRequirementExtension implements InvocationInterceptor
{
    public <T> T interceptTestClassConstructor(Invocation<T> invocation,
                                               ReflectiveInvocationContext<Constructor<T>> invocationContext,
                                               ExtensionContext extensionContext) throws Throwable
    {
        SSTableRequirement versionRequirement = extensionContext
                                                .getRequiredTestClass()
                                                .getAnnotation(SSTableRequirement.class);
        skipIfOutOfScope(versionRequirement);
        return invocation.proceed();
    }

    public void interceptTestMethod(Invocation<Void> invocation,
                                    ReflectiveInvocationContext<Method> invocationContext,
                                    ExtensionContext extensionContext) throws Throwable
    {
        interceptTestMethod(invocation, extensionContext);
    }

    public void interceptDynamicTest(Invocation<Void> invocation,
                                     DynamicTestInvocationContext invocationContext,
                                     ExtensionContext extensionContext) throws Throwable
    {
        interceptTestMethod(invocation, extensionContext);
    }

    public void interceptTestTemplateMethod(Invocation<Void> invocation,
                                            ReflectiveInvocationContext<Method> invocationContext,
                                            ExtensionContext extensionContext) throws Throwable
    {
        interceptTestMethod(invocation, extensionContext);
    }

    private void interceptTestMethod(Invocation<Void> invocation,
                                     ExtensionContext extensionContext) throws Throwable
    {
        SSTableRequirement versionRequirement = extensionContext
                                                .getRequiredTestMethod()
                                                .getAnnotation(SSTableRequirement.class);
        skipIfOutOfScope(versionRequirement);
        invocation.proceed();
    }

    private void skipIfOutOfScope(SSTableRequirement requirement)
    {
        if (requirement != null)
        {
            assumeTrue(CassandraVersion.configuredSSTableFormat().equals(requirement.format()),
                       requirement::description);
        }
    }
}
