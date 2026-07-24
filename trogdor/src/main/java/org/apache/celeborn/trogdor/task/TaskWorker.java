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

import org.apache.celeborn.trogdor.platform.Platform;

/** The agent-side interface for implementing tasks. */
public interface TaskWorker {
  /**
   * Starts the TaskWorker.
   *
   * <p>The start() implementation should return quickly. Time-consuming operations should be
   * performed in a background thread.
   *
   * <p>If start() throws an exception, the Agent will assume that the TaskWorker never started, and
   * stop() will not be invoked. If the haltFuture is completed, either by a background task or by
   * the start function itself, the Agent will invoke stop() to clean up the worker.
   *
   * @param platform The platform to use.
   * @param spec The task specification.
   * @param status The current status tracker. The TaskWorker can update this at any time.
   * @param haltFuture A future which the worker should complete when it halts. An empty string
   *     means success; a non-empty string is treated as an error message.
   * @throws Exception If the TaskWorker failed to start.
   */
  void start(
      Platform platform,
      TaskSpec spec,
      WorkerStatusTracker status,
      CompletableFuture<String> haltFuture)
      throws Exception;

  /**
   * Stops the TaskWorker.
   *
   * <p>Regardless of why the TaskWorker was stopped, stop() should release all resources and stop
   * all threads before returning.
   *
   * @param platform The platform to use.
   * @throws Exception If there was an error cleaning up the TaskWorker.
   */
  void stop(Platform platform) throws Exception;
}
