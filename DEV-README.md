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

# Cassandra Analytics

Cassandra Analytics supports Spark 3 (Scala 2.12).

This project uses Gradle as the dependency management and build framework.

## Dependencies
This library depends on both the [Cassandra Sidecar](https://github.com/apache/cassandra-sidecar) (test and production)
and shaded in-jvm dtest jars from [Cassandra](https://github.com/apache/cassandra) (testing only).
Because these artifacts are not published by the Cassandra project, we have provided a script to build them locally.

NOTE: If you are working on multiple projects that depend on the Cassandra Sidecar and in-jvm dtest dependencies,
you can share those artifacts by setting the `CASSANDRA_DEP_DIR` environment variable to a shared directory
and dependencies will build there instead of local to the project.

In order to build the necessary dependencies, please run the following:

```shell
./scripts/build-dependencies.sh
```

This will build both the necessary dtest jars and the sidecar libraries/package necessary for build and test.
You can also skip either the dtest jar build or the sidecar build by setting the following 
environment variables to `true`:

```shell
SKIP_DTEST_JAR_BUILD=true SKIP_SIDECAR_BUILD=true ./scripts/build-dependencies.sh
```

Note that `build-dependencies.sh` attempts to pull the latest from branches specified in the `BRANCHES` environment
variable for Cassandra dtest jars, and trunk for the sidecar.

## Building

Once you've built the dependencies, you're ready to build the analytics project.

Cassandra Analytics will build for Spark 3 and Scala 2.12 by default.

Navigate to the top-level directory for this project:

```shell
./gradlew clean assemble
```

### Spark 3 and Scala 2.12

To build for Spark 3 and Scala 2.12, export both `SCALA_VERSION=2.12` and `SPARK_VERSION=3`:

```shell
export SCALA_VERSION=2.12
export SPARK_VERSION=3
./gradlew clean assemble
```

### Git hooks (optional)

To enable git hooks, run the following command at project root. 

```shell
git config core.hooksPath githooks
```

## Running Integration Tests

To run integration tests, build dependencies with instructions under Dependencies section and configure IP aliases 
needed for integration tests.

### macOS network aliases
create a temporary alias for every node except the first:

```shell
 for i in {2..20}; do sudo ifconfig lo0 alias "127.0.0.${i}"; done
```

## IntelliJ

The project is well-supported in IntelliJ.

Run the following profile to copy code style used for this project:

```shell
./gradlew copyCodeStyle
```

## Bridge Modules

Major Cassandra versions require dedicated bridge modules (e.g. `cassandra-five-zero-bridge`) to be able to reference
Cassandra as a library, and correctly read sstable files. Significant part of code may be based on previous module
version. To minimise code duplication, Gradle build copies all source files from previous dependent module.
If implementation of given component has to be updated, copy the file with the same name to a target module and
modify its body.