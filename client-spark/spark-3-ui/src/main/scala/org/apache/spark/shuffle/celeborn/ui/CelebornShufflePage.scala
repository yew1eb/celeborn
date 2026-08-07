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

import scala.collection.JavaConverters._
import scala.xml.Node

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}

/**
 * Page rendered under the Celeborn tab. Uses [[TypeAlias]] to bridge the
 *  javax.servlet (Spark 3.x) / jakarta.servlet (Spark 4.x) parameter type.
 */
private[celeborn] class CelebornShufflePage(parent: CelebornUITab)
  extends WebUIPage("") with Logging {
  import org.apache.spark.shuffle.celeborn.ui.TypeAlias._

  private val statusStore = parent.statusStore

  override def render(request: HttpServletRequest): Seq[Node] = {
    val buildInfo = statusStore.buildInfo()
    val assignments = statusStore.assignmentInfos()
    val fallback = statusStore.fallbackStats()

    val summary: Seq[Node] =
      <div>
        <ul class="list-unstyled">
          <li>
            <strong>Celeborn Shuffle Service</strong>
          </li>
          <li>
            <a href="#buildinfo">Build Information</a>
          </li>
          <li>
            <a href="#assignments">Shuffle Assignments</a> ({assignments.length} shuffles)
          </li>
          <li>
            <a href="#fallback">Fallback Statistics</a>
          </li>
        </ul>
      </div>

    val buildInfoTable = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true)

    val assignmentRows = assignments.map { a =>
      <tr>
        <td>{a.appShuffleId}</td>
        <td>{a.celebornShuffleId}</td>
        <td>{a.workers.asScala.mkString(", ")}</td>
        <td>{a.numPartitions}</td>
        <td>{new java.util.Date(a.timestamp).toString}</td>
      </tr>
    }
    val assignmentTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>App Shuffle Id</th>
            <th>Celeborn Shuffle Id</th>
            <th>Assigned Workers</th>
            <th>Num Partitions</th>
            <th>Timestamp</th>
          </tr>
        </thead>
        <tbody>
          {assignmentRows}
        </tbody>
      </table>

    val fallbackRows = fallback.counts.asScala.toSeq.sortBy(_._1).map { case (policy, count) =>
      <tr>
        <td>{policy}</td>
        <td>{count}</td>
      </tr>
    }
    val fallbackTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>Fallback Policy</th>
            <th>Count</th>
          </tr>
        </thead>
        <tbody>
          {fallbackRows}
        </tbody>
      </table>

    val content =
      <div>
        <span>{summary}</span>
        <a name="buildinfo"></a>
        <h4>
          <strong>Build Information</strong>
        </h4> ++ buildInfoTable ++
        <a name="assignments"></a>
        <h4>
          <strong>Shuffle Assignments</strong>
        </h4> ++ assignmentTable ++
        <a name="fallback"></a>
        <h4>
          <strong>Fallback Statistics</strong>
        </h4> ++ fallbackTable
      </div>

    UIUtils.headerSparkPage(request, "Celeborn Shuffle Service", content, parent)
  }

  private def propertyHeader: Seq[String] = Seq("Property", "Value")

  private def propertyRow(kv: (String, String)): Seq[Node] =
    <td>{kv._1}</td>
      <td>{kv._2}</td>
}
