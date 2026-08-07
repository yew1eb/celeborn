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
    try {
      renderBody(request)
    } catch {
      case e: Throwable =>
        logError("Failed to render Celeborn Shuffle page", e)
        val errorContent =
          <div class="row-fluid">
            <div class="span12">
              <h4>
                <strong>Celeborn Shuffle Service</strong>
              </h4>
              <div class="alert alert-error">
                <pre>Failed to render the Celeborn page: {e.getMessage}</pre>
              </div>
            </div>
          </div>
        UIUtils.headerSparkPage(request, "Celeborn Shuffle Service", errorContent, parent)
    }
  }

  private def renderBody(request: HttpServletRequest): Seq[Node] = {
    val buildInfo = statusStore.buildInfo()
    val assignments = statusStore.assignmentInfos()
    val fallback = statusStore.fallbackStats()
    val properties = statusStore.celebornProperties()
    val reassign = statusStore.reassignStats()
    val writeMetrics = statusStore.aggregatedWriteMetrics()
    val taskInfo = statusStore.aggregatedTaskInfo()

    // --- Summary derived from onTaskEnd TaskMetrics (a-class, plan A) ---
    val writeBytes = taskInfo.shuffleWriteBytes
    val readBytes = taskInfo.shuffleReadBytes
    val writeMs = taskInfo.shuffleWriteTimeMs
    val readMs = taskInfo.shuffleFetchWaitTimeMs
    val cpuMs = taskInfo.taskCpuTimeMs
    def mbps(bytes: Long, ms: Long): String =
      if (ms <= 0) "N/A" else f"${bytes.toDouble / 1000.0 / 1000.0 / (ms.toDouble / 1000.0)}%.2f"
    def pct(part: Long, total: Long): String =
      if (total <= 0) "N/A" else f"${part.toDouble * 100.0 / total.toDouble}%.1f%%"

    val summary: Seq[Node] =
      <div>
        <ul class="list-unstyled">
          <li>
            <strong>Celeborn Shuffle Service</strong>
          </li>
          <li>Total Shuffle Write Bytes: {org.apache.spark.util.Utils.bytesToString(writeBytes)}</li>
          <li>Total Shuffle Read Bytes: {org.apache.spark.util.Utils.bytesToString(readBytes)}</li>
          <li>Client Observed Write Speed: {mbps(writeBytes, writeMs)} MB/s</li>
          <li>Client Observed Read Speed: {mbps(readBytes, readMs)} MB/s</li>
          <li>Shuffle Write Time / Task CPU Time: {pct(writeMs, cpuMs)}</li>
          <li>Shuffle Read Time / Task CPU Time: {pct(readMs, cpuMs)}</li>
          <li>Reassign Status: partitionSplit={reassign.partitionSplit},
            blockSendFailure={reassign.blockSendFailure}, stageRetry={reassign.stageRetry}</li>
          <li>
            <a href="#buildinfo">Build Information</a>
          </li>
          <li>
            <a href="#properties">Celeborn Properties</a> ({properties.info.length} entries)
          </li>
          <li>
            <a href="#assignments">Shuffle Assignments</a> ({assignments.length} shuffles)
          </li>
          <li>
            <a href="#fallback">Fallback Statistics</a>
          </li>
          <li>
            <a href="#throughput">Shuffle Throughput</a>
          </li>
          <li>
            <a href="#write">Per-shuffle Write Metrics</a>
          </li>
        </ul>
      </div>

    val buildInfoTable = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true)

    val propertiesTable = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      properties.info,
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

    val throughputRows = Seq(
      (
        "Total Shuffle Write Bytes",
        org.apache.spark.util.Utils.bytesToString(taskInfo.shuffleWriteBytes)),
      ("Total Shuffle Write Time (ms)", taskInfo.shuffleWriteTimeMs),
      (
        "Total Shuffle Read Bytes",
        org.apache.spark.util.Utils.bytesToString(taskInfo.shuffleReadBytes)),
      ("Total Fetch Wait Time (ms)", taskInfo.shuffleFetchWaitTimeMs),
      ("Total Task CPU Time (ms)", taskInfo.taskCpuTimeMs))
    val throughputRowsXml = throughputRows.map { case (label, value) =>
      <tr>
        <td>{label}</td>
        <td>{value.toString}</td>
      </tr>
    }
    val throughputTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>Metric</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          {throughputRowsXml}
        </tbody>
      </table>

    val writeRows = writeMetrics.metrics.asScala.toSeq.sortBy(_._1).map { case (sid, m) =>
      <tr>
        <td>{sid}</td>
        <td>{org.apache.spark.util.Utils.bytesToString(m.bytesWritten)}</td>
        <td>{m.recordsWritten}</td>
        <td>{m.writeTimeMs}</td>
      </tr>
    }
    val writeTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>Shuffle Id</th>
            <th>Bytes Written</th>
            <th>Records Written</th>
            <th>Write Time (ms)</th>
          </tr>
        </thead>
        <tbody>
          {writeRows}
        </tbody>
      </table>

    val content: Seq[Node] =
      <div><span>{summary}</span></div> ++
        <a name="buildinfo"></a> ++
        <h4><strong>Build Information</strong></h4> ++ buildInfoTable ++
        <a name="properties"></a> ++
        <h4><strong>Celeborn Properties</strong></h4> ++ propertiesTable ++
        <a name="assignments"></a> ++
        <h4><strong>Shuffle Assignments</strong></h4> ++ assignmentTable ++
        <a name="fallback"></a> ++
        <h4><strong>Fallback Statistics</strong></h4> ++ fallbackTable ++
        <a name="throughput"></a> ++
        <h4><strong>Shuffle Throughput</strong></h4> ++ throughputTable ++
        <a name="write"></a> ++
        <h4><strong>Per-shuffle Write Metrics</strong></h4> ++ writeTable

    UIUtils.headerSparkPage(request, "Celeborn Shuffle Service", content, parent)
  }

  private def propertyHeader: Seq[String] = Seq("Property", "Value")

  private def propertyRow(kv: (String, String)): Seq[Node] =
    <tr>
      <td>{kv._1}</td>
      <td>{kv._2}</td>
    </tr>
}
