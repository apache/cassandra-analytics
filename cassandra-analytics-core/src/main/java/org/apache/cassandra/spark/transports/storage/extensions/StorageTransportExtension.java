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

package org.apache.cassandra.spark.transports.storage.extensions;

import org.apache.spark.SparkConf;

/**
 * The facade interface defines the contract of the extension for cloud storage data transport.
 * It servers as the integration point for the library consumer.
 * - Register callbacks for data transport progress
 * - Supply necessary information, e.g. credentials, bucket, etc., to conduct a successful data transport
 * <br>
 * Notes for the interface implementors:
 * -  Not all methods defined in the interface are invoked in both Spark driver and executors.
 *    1. The methods in {@link CommonStorageTransportExtension} are invoked in both places.
 *    2. The methods in {@link ExecutorStorageTransportExtension} are invoked in Spark executors only.
 *    3. The methods in {@link DriverStorageTransportExtension} are invoked in Spark driver only.
 * -  The Analytics library guarantees the following sequence in Spark driver on initialization
 *    1. Create the new {@link StorageTransportExtension} instance
 *    2. Invoke {@link #initialize(String, SparkConf, boolean)}
 *    3. Invoke {@link #getStorageConfiguration()}
 *    4. Invoke {@link #setCredentialChangeListener(CredentialChangeListener)}
 *    5. Invoke {@link #setObjectFailureListener(ObjectFailureListener)}
 *    6. Invoke {@link #setCoordinationSignalListener(CoordinationSignalListener)}
 */
public interface StorageTransportExtension extends CommonStorageTransportExtension,
                                                   ExecutorStorageTransportExtension,
                                                   DriverStorageTransportExtension
{
    // No extra methods to be defined
    // When adding a new one, please determine its call-site, and add it in one of the three interfaces
}
