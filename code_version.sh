#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if [[ -n "$(awk -F'[ \t]*=[ \t]*' '/^version/ { print $2; exit }' gradle.properties 2>/dev/null)" ]]; then
    version=$(awk -F'[ \t]*=[ \t]*' '/^version/ { print $2; exit }' gradle.properties)
    from_statement="from 'gradle.properties'"
elif [[ -n "$(awk -F'[<>]' 'NR>1 && /^[ \t]*<[^!]/ && /<version/ { print $3; exit }' pom.xml 2>/dev/null)" ]]; then
    version=$(awk -F'[<>]' 'NR>1 && /^[ \t]*<[^!]/ && /<version/ { print $3; exit }' pom.xml)
    from_statement="from 'pom.xml'"
elif [[ -n "$(awk '/defproject/ { gsub("\"|-SNAPSHOT", ""); print $3 }' project.clj 2>/dev/null)" ]]; then
    version=$(awk '/defproject/ { gsub("\"|-SNAPSHOT", ""); print $3 }' project.clj)
    from_statement="from 'project.clj'"
elif [[ -f version ]]; then
    version=$(cat version)
    from_statement="from toplevel 'version' file"
else
    version=$(git rev-parse --short=12 HEAD)
    from_statement="from git hash (fallback method)"
fi

# Remove quotes from the version if present
version=${version//\"/}
version=${version//\'/}

echo "Parsed version $from_statement: '$version'" 1>&2

echo "$version"
