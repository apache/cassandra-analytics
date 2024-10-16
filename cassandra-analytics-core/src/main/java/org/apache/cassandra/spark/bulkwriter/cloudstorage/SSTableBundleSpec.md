<!--
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
-->

# SSTable Bundle Specification

When bulk writing SSTables using the `S3_COMPAT` mode, the writer groups several SSTables into a single file, named `Bundle`. 

This document walks through the specification of `Bundle`. 

## Bundle Structure

A `Bundle` is essentially a zip file.

Each bundle consists of a `BundleManifest` file and sstable files. The manifest file describes the bundled sstable files, and it is used for validation.

Visually, the bundle structure is illustrated in the following tree diagram

```Plain Text
bundle/
├── manifest.json
├── sstable-1
├── sstable-2
└── ...
```

> ℹ️ A sstable consists of several components/files

## Manifest Schema

A manifest file is in `JSON` format. It is essentially a map of `<SSTableId, SSTableMetadata>`. 
`SSTableId` is the shared prefix of the components of a sstable. `SSTableMetadata` consists of a string map `componentsChecksum`, `startToken` and `endToken`.

> ℹ️ The checksum value of the individual component is computed using XXHash32

Here is an example of the manifest

```json
{
  "nb-1-big-" : {
    "start_token" : 1,
    "end_token" : 3,
    "components_checksum" : {
      "nb-1-big-Data.db" : "12345678",
      "nb-1-big-Index.db" : "12345678",
      "nb-1-big-CompressionInfo.db" : "12345678",
      "nb-1-big-Summary.db" : "12345678",
      ...
    }
  },
  "nb-2-big-" : {
    "start_token" : 4,
    "end_token" : 7,
    "components_checksum" : {
      "nb-2-big-Data.db" : "12345678",
      "nb-2-big-Index.db" : "12345678",
      "nb-2-big-CompressionInfo.db" : "12345678",
      "nb-2-big-Summary.db" : "12345678",
      ...
    },
    ...
  }
}
```
