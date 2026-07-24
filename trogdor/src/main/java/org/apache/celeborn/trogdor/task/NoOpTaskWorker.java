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

package org.apache.celeborn.trogdor.task;

import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.node.TextNode;

import org.apache.celeborn.trogdor.platform.Platform;

/** A no-op task worker that completes immediately. */
public class NoOpTaskWorker implements TaskWorker {
  @Override
  public void start(
      Platform platform,
      TaskSpec spec,
      WorkerStatusTracker status,
      CompletableFuture<String> haltFuture)
      throws Exception {
    status.update(new TextNode("no-op worker started"));
    haltFuture.complete("");
  }

  @Override
  public void stop(Platform platform) throws Exception {}
}
