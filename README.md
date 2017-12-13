<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

[![Build Status](https://travis-ci.org/ShiftLeftSecurity/tinkergraph-gremlin.svg?branch=master)](https://travis-ci.org/ShiftLeftSecurity/tinkergraph-gremlin)

# tinkergraph-gremlin

## Releasing
Just change the version in `pom.xml` to a non-snapshot (e.g. `3.3.0.3`), commit and tag it (e.g. `v3.3.0.3`). Then change the version to the next snapshot (e.g. `3.3.0.4-SNAPSHOT`), commit and push everything (including the tag!). Travis will automatically deploy the tagged version to sonatype and stage it so that it'll be synchronized to maven central within a few hours. 
