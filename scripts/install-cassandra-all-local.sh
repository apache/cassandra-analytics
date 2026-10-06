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
# Builds cassandra-all from an already checked-out Cassandra source tree and installs it into a
# local Maven repository, so that the bulk-writer bridge (for e.g. cassandra-six-zero) is compiled against
# the same Cassandra code that the pinned in-JVM dtest cluster runs.
#
# Why this is needed: build-dtest-jars.sh can pin a commit-ish that is not a released tag (currently
# a cep-45-mutation-tracking SHA for cassandra-6.0). The dtest jar only replaces the server side -
# the bridge keeps compiling against the released cassandra-all from Maven Central. That is fine for
# server-only changes, but not when the pinned commit changes for e.g. SSTable format: the writer then
# produces SSTables the server refuses to open and bulk-write integration test fails at
# SSTable import (500).
#
# Usage: install-cassandra-all-local.sh <cassandra-source-dir> <branch> <local-maven-repo-dir>
#
# The version to install is read from gradle.properties (cassandra<major><minor>Version), so what
# gets installed is by construction what Gradle asks for. Nothing is installed while that property
# still matches the source tree's own base.version, because the release on Maven Central is then the
# right artifact to use.

set -xe

SRC_DIR="$1"
BRANCH="$2"
LOCAL_REPO="$3"

if [ -z "${SRC_DIR}" ] || [ -z "${BRANCH}" ] || [ -z "${LOCAL_REPO}" ]; then
  echo "Usage: $0 <cassandra-source-dir> <branch> <local-maven-repo-dir>"
  exit 1
fi

SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
ROOT_DIR=$( dirname "${SCRIPT_DIR}" )

# cassandra-6.0 -> cassandra60Version
VERSION_PROPERTY="cassandra$(echo "${BRANCH}" | sed -e 's/^cassandra-//' -e 's/\.//g')Version"
CONFIGURED_VERSION=$(grep -E "^${VERSION_PROPERTY}=" "${ROOT_DIR}/gradle.properties" | head -1 | cut -d'=' -f2 | tr -d '[:space:]')

if [ -z "${CONFIGURED_VERSION}" ]; then
  echo "Could not read ${VERSION_PROPERTY} from ${ROOT_DIR}/gradle.properties"
  exit 1
fi

cd "${SRC_DIR}"
BASE_VERSION=$(cat build.xml | grep 'property name="base.version"' | awk -F "\"" '{print $4}')

if [ "${CONFIGURED_VERSION}" == "${BASE_VERSION}" ]; then
  echo "${VERSION_PROPERTY}=${CONFIGURED_VERSION} matches base.version of $(pwd); the released"
  echo "cassandra-all from Maven Central is the artifact to use, nothing to install."
  exit 0
fi

ARTIFACT_DIR="${LOCAL_REPO}/org/apache/cassandra/cassandra-all/${CONFIGURED_VERSION}"

# 'ant mvn-install' installs into ~/.m2; copy the artifacts over so they land in the repository
# Gradle reads (see the mavenLocal block in build.gradle, whose url is the dependencies directory).
# cassandra-parent is copied as well because the cassandra-all pom names it as its parent, and Gradle
# has to resolve that pom too; cassandra-accord because the cassandra-all pom depends on it and the
# accord submodule publishes it from the same commit (build.xml leaves it to the submodule build).
copy_artifacts_from_m2() {
  for artifact in cassandra-all cassandra-parent cassandra-accord; do
    if [ ! -f "${LOCAL_REPO}/org/apache/cassandra/${artifact}/${CONFIGURED_VERSION}/${artifact}-${CONFIGURED_VERSION}.pom" ]; then
      installed="${HOME}/.m2/repository/org/apache/cassandra/${artifact}/${CONFIGURED_VERSION}"
      if [ -d "${installed}" ]; then
        mkdir -p "${LOCAL_REPO}/org/apache/cassandra/${artifact}"
        cp -R "${installed}" "${LOCAL_REPO}/org/apache/cassandra/${artifact}/"
      fi
    fi
  done
}

# A previous run may have installed cassandra-all without every artifact the pom pulls in, so top the
# copy up before deciding there is nothing left to do.
copy_artifacts_from_m2

if [ -f "${ARTIFACT_DIR}/cassandra-all-${CONFIGURED_VERSION}.jar" ]; then
  echo "cassandra-all ${CONFIGURED_VERSION} is already installed in ${LOCAL_REPO}"
  exit 0
fi

# Overriding base.version keeps the build self-consistent: the jar name, the generated poms and the
# installed coordinates all carry ${CONFIGURED_VERSION}, so no pom has to be rewritten afterwards.
# Keep the build qualifier dot-separated (6.0-alpha2.<sha>, not 6.0-alpha2-<sha>) - a second dash
# does not parse as a Cassandra release version.
# local.repository is deliberately left at its default (~/.m2/repository): build.xml reads artifacts
# back out of it (e.g. it unzips the jacoco agent from
# ${local.repository}/org/jacoco/org.jacoco.agent) while maven-resolver-ant-tasks writes them to
# ~/.m2/repository regardless of that property, so pointing it at ${LOCAL_REPO} fails the build.
# The copy step below moves the installed artifacts into ${LOCAL_REPO} instead.
# release=true is what makes build.xml use ${base.version} verbatim as the artifact version; without
# it the build appends -SNAPSHOT and the installed coordinates no longer match the version Gradle
# asks for.
ant mvn-install -Dbase.version="${CONFIGURED_VERSION}" \
                -Drelease=true \
                -Dno-checkstyle=true \
                -Dno-javadoc=true \
                -Dant.gen-doc.skip=true \
                ${CASSANDRA_ANT_FLAGS}

copy_artifacts_from_m2

if [ ! -f "${ARTIFACT_DIR}/cassandra-all-${CONFIGURED_VERSION}.jar" ]; then
  echo "cassandra-all ${CONFIGURED_VERSION} did not end up in ${LOCAL_REPO}."
  echo "Read the 'ant mvn-install' output above; to finish by hand:"
  echo "  cd ${SRC_DIR}"
  echo "  ant mvn-install -Dbase.version=${CONFIGURED_VERSION}"
  echo "  mkdir -p ${LOCAL_REPO}/org/apache/cassandra/cassandra-all"
  echo "  cp -R ~/.m2/repository/org/apache/cassandra/cassandra-all/${CONFIGURED_VERSION} \\"
  echo "        ${LOCAL_REPO}/org/apache/cassandra/cassandra-all/"
  exit 1
fi

# A cassandra-all pom that declares a parent is unresolvable without that parent pom, and the parent
# is not on Maven Central for a version built here.
if grep -q '<parent>' "${ARTIFACT_DIR}/cassandra-all-${CONFIGURED_VERSION}.pom" 2>/dev/null &&
   [ ! -f "${LOCAL_REPO}/org/apache/cassandra/cassandra-parent/${CONFIGURED_VERSION}/cassandra-parent-${CONFIGURED_VERSION}.pom" ]; then
  echo "WARNING: cassandra-all-${CONFIGURED_VERSION}.pom declares a parent, but"
  echo "cassandra-parent-${CONFIGURED_VERSION}.pom is not in ${LOCAL_REPO}; Gradle will fail to"
  echo "resolve the module. Install the parent pom from the build as well, e.g.:"
  echo "  cd ${SRC_DIR} && ant mvn-install -Dbase.version=${CONFIGURED_VERSION}"
  echo "  cp -R ~/.m2/repository/org/apache/cassandra/cassandra-parent/${CONFIGURED_VERSION} \\"
  echo "        ${LOCAL_REPO}/org/apache/cassandra/cassandra-parent/"
fi

echo "Installed cassandra-all ${CONFIGURED_VERSION} (built from $(git rev-parse HEAD 2>/dev/null || echo unknown)) into ${LOCAL_REPO}"
