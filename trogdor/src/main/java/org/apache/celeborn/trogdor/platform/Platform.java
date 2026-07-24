/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.trogdor.platform;

import java.io.Closeable;

/** The platform abstraction used by Trogdor tasks to interact with the environment. */
public interface Platform extends Closeable {
  /** The topology of nodes. */
  Topology topology();

  /** The node on which the current process is running. */
  Node curNode();

  /**
   * Run a command on the given node.
   *
   * @param nodeName The target node name.
   * @param command The command to run.
   * @return The command output.
   * @throws Exception if the command fails.
   */
  String runCommand(String nodeName, String[] command) throws Exception;
}
