<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Collections Example

## Cassandra Schema

```
CREATE TABLE examples.collections (
    a_text text,
    a_set set<int>,
    a_list list<uuid>,
    a_map map<bigint, text>,
    PRIMARY KEY(a_text)
)
```

## INSERT Example

An INSERT into all primary keys and value columns.

### Avro Header

```
{
  "timestampMicros" : 1711149035739000,
  "sourceTable" : "collections",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "9776a3e3-1f9e-3351-a6cd-f920f6fb9d75",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "INSERT",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a_text", "a_list", "a_map", "a_set" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a_text" : {
    "string" : "ObC1n1QmRvRs95VgRwjAgtOzgkmj"
  },
  "a_list" : {
    "array" : [ "e8b50352-2a69-4afe-a1f4-53415deaaa79", "d1ee3c30-a432-480b-b0c4-6af53d75004f", "ce236fa7-3fd0-4f87-9199-441a92ef7058", "7e96d13c-967e-4664-ac66-6dd9034a6caa", "34f94b2f-d350-439d-9d6b-86731495c688", "a81a4f47-91e8-4a78-a4b3-7a0188d5d1a0", "1e5d5ca8-9cf6-4a60-8029-c250f730c5e7", "4c39bbbe-6394-4fbd-9d58-8f57b27627ed", "18f7ac97-aaaf-49b1-a26a-b3b7510782ec", "82fb753c-f9f8-4b13-9b4c-558af168e350", "23c1ba5b-5e9f-46f1-871f-bb7342e2d6a6", "3133987d-5e84-4fc3-987a-4df52bc21b10" ]
  },
  "a_map" : {
    "array" : [ {
      "key" : 242321,
      "value" : "EK5UcLLaYHLKmEVcG"
    }, {
      "key" : 312814,
      "value" : "iAT278KZsrr7FtV"
    }, {
      "key" : 569232,
      "value" : "RhSwbD4P383MJaWSg0rKh"
    }, {
      "key" : 682790,
      "value" : "4oB32DXoAVYl9C"
    }, {
      "key" : 707529,
      "value" : "ThqZnX"
    }, {
      "key" : 1036907,
      "value" : "NGDEjAKJwTs6AI"
    }, {
      "key" : 1079240,
      "value" : "eoClW99xI7D5CUq"
    }, {
      "key" : 2179195,
      "value" : "NqY"
    }, {
      "key" : 2280983,
      "value" : "H0TAPPg1EFgKnYtkzgExgrjx13QX"
    }, {
      "key" : 2498012,
      "value" : "bXtS"
    }, {
      "key" : 2886009,
      "value" : "I"
    }, {
      "key" : 2962102,
      "value" : "CF9FQFTeX2VBIT7yghnLd9k"
    }, {
      "key" : 4074524,
      "value" : "KuyU5xvY"
    }, {
      "key" : 4213066,
      "value" : "y2QW7"
    }, {
      "key" : 4385643,
      "value" : "NkM6eYY00H"
    } ]
  },
  "a_set" : {
    "array" : [ -1850144914, -1820035823, -1797347611, -1685557604, -1599613798, -1345098995, -994104589, -972293759, -910033848, -680773429, -660297466, -505037467, 298790243, 591905756, 1169645054, 1744123637, 2057007774, 2094751676 ]
  }
}
```

## UPDATE Example

An UPDATE into all primary keys and a single value column.

### Avro Header

```
{
  "timestampMicros" : 1711149035742000,
  "sourceTable" : "collections",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "9776a3e3-1f9e-3351-a6cd-f920f6fb9d75",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "UPDATE",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a_text", "a_list" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a_text" : {
    "string" : "b2MyHzzlq3rCv"
  },
  "a_list" : {
    "array" : [ "033d691b-a8e4-4d80-88f9-0880d0f7b82a", "25cd5ce8-cec6-49f3-a7ac-dac6039ee352", "0e961dd9-0596-46ac-a376-30fe9787a7f0", "0ff6d893-20b8-40a6-8b3f-224c15a78658" ]
  },
  "a_map" : null,
  "a_set" : null
}
```

## DELETE Example

A point DELETE of a single value column.

### Avro Header

```
{
  "timestampMicros" : 1711149035743000,
  "sourceTable" : "collections",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "9776a3e3-1f9e-3351-a6cd-f920f6fb9d75",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a_text", "a_list" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a_text" : {
    "string" : "D"
  },
  "a_list" : null,
  "a_map" : null,
  "a_set" : null
}
```

## PARTITION_DELETE Example

A PARTITION_DELETE that deletes using only the partition keys.

### Avro Header

```
{
  "timestampMicros" : 1711149035744000,
  "sourceTable" : "collections",
  "sourceKeyspace" : "examples",
  "schemaUuid" : "9776a3e3-1f9e-3351-a6cd-f920f6fb9d75",
  "truncatedFields" : [ ],
  "version" : {
    "string" : "2"
  },
  "operationType" : "DELETE_PARTITION",
  "isPartial" : true,
  "updateFields" : {
    "array" : [ "a_text" ]
  },
  "range" : null,
  "ttl" : null,
  "payload": "[binary serialized value of payload below]"
}
```



### Avro Payload

```
{
  "a_text" : {
    "string" : "xJx"
  },
  "a_list" : null,
  "a_map" : null,
  "a_set" : null
}
```

