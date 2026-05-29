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

package org.apache.cassandra.spark.transports.storage;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * {@link StorageAuth} implementation for IAM instance profile / IRSA / ECS task role authentication.
 * Carries no static credentials; the AWS SDK resolves them automatically via its default provider chain.
 * Use {@link #INSTANCE} — there is no per-instance state.
 */
public final class IamStorageAuth implements StorageAuth
{
    public static final IamStorageAuth INSTANCE = new IamStorageAuth();

    private IamStorageAuth()
    {
    }

    @Override
    public AwsCredentialsProvider toAwsCredentialsProvider()
    {
        return DefaultCredentialsProvider.create();
    }

    @Override
    public String toString()
    {
        return "IamStorageAuth";
    }
}
