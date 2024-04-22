<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# Sample Cassandra Data with Spark Bulk Analytics Job

This sub-project showcases the Cassandra Spark Bulk Analytics read and write functionality. The job writes data to a
Cassandra table using the bulk writer functionality, and then it reads the data using the bulk reader functionality.

## Requirements

- Java 11
- A running Cassandra 4.0 cluster
- A running Cassandra Sidecar service

## Setup

This example requires a Cassandra 4.0 cluster running, we recommend a 3-node cluster. Additionally, it requires the
latest version of Cassandra Sidecar that we will run from source.

### Step 1: Cassandra using CCM

The easiest way to provision a Cassandra cluster is by using Cassandra Cluster Manager or
[CCM](https://github.com/riptano/ccm). Follow the configuration instructions for CCM and provision a new test cluster.

For example,

```shell
ccm create test --version=4.1.1 --nodes=3
```

> **Note**
> If you are using macOS, explicit configurations of interface aliases are needed as follows:

```shell
sudo ifconfig lo0 alias 127.0.0.2
sudo ifconfig lo0 alias 127.0.0.3
sudo bash -c 'echo "127.0.0.2  localhost2" >> /etc/hosts'
sudo bash -c 'echo "127.0.0.3  localhost3" >> /etc/hosts'
```

After configuring the network interfaces run:

```shell
ccm start
```

Verify that your Cassandra cluster is configured and running successfully.

### Step 2: Configure and Run Cassandra Sidecar

In this step, we will clone and configure the Cassandra Sidecar project. Finally, we will run Sidecar which will be
connecting to our local Cassandra 3-node cluster.

Note that when building the main project, you should have run the `./scripts/build-dependencies.sh` script,
which would have cloned and built the sidecar into `./dependencies/sidecar-build`. Use that build to run the sidecar.

```shell
cd ./dependencies/sidecar-build/trunk
```

Configure the `src/main/dist/sidecar.yaml` file for your local environment. You will most likely only need to configure
the `cassandra_instances` section in your file pointing to your local Cassandra data directories. Here is what my
`cassandra_instances` configuration section looks like for this tutorial:

```yaml
cassandra_instances:
  - id: 1
    host: localhost
    port: 9042
    data_dirs: <your_ccm_parent_path>/.ccm/test/node1/data0
    staging_dir: <your_ccm_parent_path>/.ccm/test/node1/sstable-staging
    jmx_host: 127.0.0.1
    jmx_port: 7100
    jmx_ssl_enabled: false
  - id: 2
    host: localhost2
    port: 9042
    data_dirs: <your_ccm_parent_path>/.ccm/test/node2/data0
    staging_dir: <your_ccm_parent_path>/.ccm/test/node2/sstable-staging
    jmx_host: 127.0.0.1
    jmx_port: 7200
    jmx_ssl_enabled: false
  - id: 3
    host: localhost3
    port: 9042
    data_dirs: <your_ccm_parent_path>/.ccm/test/node3/data0
    staging_dir: <your_ccm_parent_path>/.ccm/test/node3/sstable-staging
    jmx_host: 127.0.0.1
    jmx_port: 7300
    jmx_ssl_enabled: false
```

I have a 3 node setup, so I configure Sidecar for those 3 nodes. CCM creates the Cassandra cluster under
`${HOME}/.ccm/test`, so I update my `data_dirs` and `staging_dir` configuration to use my local path.

Finally, run Cassandra Sidecar. We skip running integration tests because the integration tests take quite some time.
You can, of course, choose to run them (and should when working on the sidecar the project itself).

```shell
user:~$ ./gradlew run -x test -x integrationTest -x containerTest
...

> Task :run
Could not start Jolokia agent: java.net.BindException: Address already in use
WARNING: An illegal reflective access operation has occurred
WARNING: Please consider reporting this to the maintainers of com.google.inject.internal.cglib.core.$ReflectUtils$1
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
INFO  [main] 2023-04-13 13:13:35,939 YAMLSidecarConfiguration.java:211 - Reading configuration from file://${HOME}/workspace/cassandra-sidecar-server/conf/sidecar.yaml
INFO  [main] 2023-04-13 13:13:35,997 CQLSessionProvider.java:63 - Connecting to localhost on port 9042
INFO  [main] 2023-04-13 13:13:36,001 CQLSessionProvider.java:63 - Connecting to localhost2 on port 9042
INFO  [main] 2023-04-13 13:13:36,014 CQLSessionProvider.java:63 - Connecting to localhost3 on port 9042
INFO  [main] 2023-04-13 13:13:36,030 CacheFactory.java:83 - Building SSTable Import Cache with expireAfterAccess=PT2H, maxSize=10000
 _____                               _              _____ _     _                     
/  __ \                             | |            /  ___(_)   | |                    
| /  \/ __ _ ___ ___  __ _ _ __   __| |_ __ __ _   \ `--. _  __| | ___  ___ __ _ _ __ 
| |    / _` / __/ __|/ _` | '_ \ / _` | '__/ _` |   `--. \ |/ _` |/ _ \/ __/ _` | '__|
| \__/\ (_| \__ \__ \ (_| | | | | (_| | | | (_| |  /\__/ / | (_| |  __/ (_| (_| | |   
 \____/\__,_|___/___/\__,_|_| |_|\__,_|_|  \__,_|  \____/|_|\__,_|\___|\___\__,_|_|
                                                                                      
                                                                                      
INFO  [main] 2023-04-13 13:13:36,229 CassandraSidecarDaemon.java:75 - Starting Cassandra Sidecar on 0.0.0.0:9043
```

There we have it, Cassandra Sidecar is now running and connected to all 3 Cassandra nodes on my local machine.

### Step 3: Run the Sample Job

To be able to run the [Sample Job](./src/main/java/org/apache/cassandra/spark/example/DirectWriteAndReadJob.java), you
need to create the keyspace and table used for the test.

Connect to your local Cassandra cluster using CCM:

```shell
ccm node1 cqlsh
```

Then run the following command to create the keyspace:

```cassandraql
CREATE KEYSPACE spark_test WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}
                            AND durable_writes = true;
```

Then run the following command to create the table:

```cassandraql
CREATE TABLE spark_test.test
(
    id     BIGINT PRIMARY KEY,
    course BLOB,
    marks  BIGINT
);
```

#### Start DirectWriteAndReadJob

Finally, we are ready to run the example spark job:

```shell
cd ${ANALYTICS_REPOSITORY_HOME}
./gradlew :cassandra-analytics-core-example:run
# or this command
# ./gradlew :cassandra-analytics-core-example:run --args='DirectWriteAndReadJob'
```

#### Start LocalS3WriteAndReadJob

Alternatively, we can run the [LocalS3CassandraWriteJob](./src/main/java/org/apache/cassandra/spark/example/LocalS3WriteAndReadJob.java), which bulk writes
data via S3. In order to run such job, there is two additional prerequisite steps.

Start S3Mock

```shell
docker run -p 127.0.0.1:9090:9090 -p 127.0.0.1:9191:9191 -t adobe/s3mock:2.17.0
```

Restart sidecar with the following edits for `sidecar.yaml`. 
It is required to enable sidecar schema and point s3 client to the S3Mock with the endpoint_override.

```yaml
sidecar:
  schema:
    is_enabled: true
...
s3_client:
  proxy_config:
    endpoint_override: localhost:9090
```

Then, we can run the example spark job:

```shell
cd ${ANALYTICS_REPOSITORY_HOME}
./gradlew :cassandra-analytics-core-example:run --args='LocalS3WriteAndReadJob'
```

## Tear down

Stop the Cassandra Sidecar project and tear down the ccm cluster

```shell
ccm remove test
```
