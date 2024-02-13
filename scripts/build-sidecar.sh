#!/bin/bash
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

set -ex
if [ "${SKIP_SIDECAR_BUILD}" == "true" ]; then
  echo "Skipping Sidecar Build because SKIP_SIDECAR_BUILD was set to 'true'"
else
  SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
  SIDECAR_REPO="${SIDECAR_REPO:-https://github.com/apache/cassandra-sidecar.git}"
  SIDECAR_BRANCH="${SIDECAR_BRANCH:-trunk}"
  SIDECAR_COMMIT="${SIDECAR_COMMIT:-2dcbca66a9f6a1e6b7cf4bd2c4f1368cc2ede86a}"
  SIDECAR_JAR_DIR="$(dirname "${SCRIPT_DIR}/")/dependencies"
  SIDECAR_JAR_DIR=${CASSANDRA_DEP_DIR:-$SIDECAR_JAR_DIR}
  SIDECAR_BUILD_DIR="${SIDECAR_JAR_DIR}/sidecar-build"
  java_ver_output=`"${JAVA:-java}" -version 2>&1`
  jvmver=`echo "$java_ver_output" | grep '[openjdk|java] version' | awk -F'"' 'NR==1 {print $2}' | cut -d\- -f1`
  JVM_VERSION=${jvmver%_*}
  echo $JVM_VERSION
  if [ "${JVM_VERSION}" == "1.8.0" ]; then
    SIDECAR_BUILD_VERSION="1.0.0-jdk8-analytics"
  else
    SIDECAR_BUILD_VERSION="1.0.0-analytics"
  fi
  if [[ "$CLEAN" == "true" ]]; then
    echo "Clean up $SIDECAR_BUILD_DIR and $SIDECAR_JAR_DIR/org/apache/cassandra/(cassandra-)sidecar directories"
    rm -rf "${SIDECAR_BUILD_DIR}"
    rm -rf "${SIDECAR_JAR_DIR}/org/apache/cassandra/sidecar"
    rm -rf "${SIDECAR_JAR_DIR}/org/apache/cassandra/cassandra-sidecar"
  fi
  mkdir -p "${SIDECAR_BUILD_DIR}"
  cd "${SIDECAR_BUILD_DIR}"
  echo "branch ${SIDECAR_BRANCH} sha ${SIDECAR_COMMIT}"
  # check out the correct cassandra version:
  if [ ! -d "${SIDECAR_BRANCH}" ] ; then
    if [ -n "${SIDECAR_COMMIT}" ] ; then
      mkdir -p "${SIDECAR_BRANCH}"
      cd "${SIDECAR_BRANCH}"
      git init
      git remote add upstream "${SIDECAR_REPO}"
      git fetch --depth=1 upstream "${SIDECAR_COMMIT}"
      git reset --hard FETCH_HEAD
    else
      git clone --depth 1 --single-branch --branch "${SIDECAR_BRANCH}" "${SIDECAR_REPO}" "${SIDECAR_BRANCH}"
      cd "${SIDECAR_BRANCH}"
    fi
  else
    cd "${SIDECAR_BRANCH}"
    if [ -z "${SIDECAR_COMMIT}" ] ; then
      git pull
    fi
  fi
  if [ -z "${SIDECAR_COMMIT}" ] ; then
    git checkout "${SIDECAR_BRANCH}"
  else
    git checkout "${SIDECAR_COMMIT}"
  fi
  git clean -fd
  ./gradlew -Pversion=${SIDECAR_BUILD_VERSION} -Dmaven.repo.local=${SIDECAR_JAR_DIR} publishToMavenLocal
fi
