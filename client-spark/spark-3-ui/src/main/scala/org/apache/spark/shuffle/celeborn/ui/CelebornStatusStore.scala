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

package org.apache.spark.shuffle.celeborn.ui

import org.apache.spark.status.ElementTrackingStore

/**
 * Thin wrapper over Spark's status KVStore exposing typed read accessors for the
 *  Celeborn UI entities written by [[CelebornListener]].
 */
private[celeborn] class CelebornStatusStore(val store: ElementTrackingStore) {

  /** Build info summary, or an empty entity if not yet written. */
  def buildInfo(): CelebornBuildInfoUIData = {
    val kClass = classOf[CelebornBuildInfoUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornBuildInfoUIData(Seq.empty)
    }
  }
}
