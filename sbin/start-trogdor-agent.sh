#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Starts a Trogdor agent on the machine this script is executed on.

if [ -z "${CELEBORN_HOME}" ]; then
  export CELEBORN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CELEBORN_HOME}/sbin/load-celeborn-env.sh"

if [ "$CELEBORN_TROGDOR_AGENT_MEMORY" = "" ]; then
  CELEBORN_TROGDOR_AGENT_MEMORY="1g"
fi

CELEBORN_JAVA_OPTS="$CELEBORN_TROGDOR_AGENT_JAVA_OPTS"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS -Xmx$CELEBORN_TROGDOR_AGENT_MEMORY"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS -Dio.netty.tryReflectionSetAccessible=true"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS -Dio.netty.allocator.type=pooled"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS -Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE"
export CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS"

exec "${CELEBORN_HOME}/sbin/celeborn-daemon.sh" start org.apache.celeborn.trogdor.service.TrogdorAgentApp 1 "$@"
