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

/**
 * Authentication mechanism used to access cloud storage.
 *
 * <ul>
 *   <li>{@link StorageCredentials} — explicit STS access key / secret / session token</li>
 *   <li>{@link IamStorageAuth} — IAM instance profile / IRSA / ECS task role;
 *       no static credentials; the AWS SDK resolves credentials automatically</li>
 * </ul>
 *
 * <p>Callers (e.g. {@code StorageClient}) dispatch via {@link #toAwsCredentialsProvider()}
 * without branching on the concrete type. New auth mechanisms are added by implementing
 * this interface — no changes to calling code are required.
 */
public interface StorageAuth
{
    /**
     * Returns an {@link AwsCredentialsProvider} for this auth mechanism.
     * The result is cached by the caller; this method may be invoked at most once per distinct instance.
     */
    AwsCredentialsProvider toAwsCredentialsProvider();
}
