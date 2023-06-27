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

Cassandra Analytics supports Spark 2 (Scala 2.11 and 2.12) and Spark 3 (Scala 2.12).

This project uses Gradle as the dependency management and build framework.

## Building

Cassandra Analytics will build for Spark 2 and Scala 2.11 by default.

Navigate to the top-level directory for this project:

```shell
./gradlew clean package
```

### Spark 2 and Scala 2.12

To build for Scala 2.12, set the profile by exporting `SCALA_VERSION=2.12`:

```shell
export SCALA_VERSION=2.12
./gradlew clean package
```

### Spark 3 and Scala 2.12

To build for Spark 3 and Scala 2.12, export both `SCALA_VERSION=2.12` and `SPARK_VERSION=3`:

```shell
export SCALA_VERSION=2.12
export SPARK_VERSION=3
./gradlew clean package
```

### Git hooks (optional)

To enable git hooks, run the following command at project root. 

```shell
git config core.hooksPath githooks
```

## IntelliJ

The project is well-supported in IntelliJ.

Run the following profile to copy code style used for this project:

```shell
./gradlew copyCodeStyle
```

The project has different sources for Spark 2 and Spark 3.

Spark 2 uses the `org.apache.spark.sql.sources.v2` APIs that have been deprecated in Spark 3.

Spark 3 uses new APIs that live in the `org.apache.spark.sql.connector.read` namespace.

By default, the project will load Spark 2 sources, but you can switch between sources by modifying the `gradle.properties` file.

For Spark 3, use the following in `gradle.properties`:

```properties
scala=2.12
spark=3
```

And then load Gradle changes (on Mac, the shortcut to load Gradle changes is <kbd>Command</kbd> + <kbd>Shift</kbd> + <kbd>I</kbd>).

This will make the IDE pick up the Spark 3 sources, and you should now be able to develop against Spark 3 as well.
