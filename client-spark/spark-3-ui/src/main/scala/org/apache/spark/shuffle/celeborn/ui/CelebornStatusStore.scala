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
import org.apache.spark.util.kvstore.KVStoreView

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

  /** All recorded shuffle assignments (shuffle -> worker topology), newest last. */
  def assignmentInfos(): Seq[CelebornShuffleAssignmentUIData] = {
    viewToSeq(store.view(classOf[CelebornShuffleAssignmentUIData]))
  }

  private def viewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    import scala.collection.JavaConverters._
    org.apache.spark.util.Utils.tryWithResource(view.closeableIterator())(iter =>
      iter.asScala.toList)
  }
}
