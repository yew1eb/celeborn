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
 * Page rendered under the Celeborn tab. Uses [[SparkServletBridge]] to bridge the
 *  javax.servlet (Spark 3.x) / jakarta.servlet (Spark 4.x) parameter type.
 */
private[celeborn] class CelebornShufflePage(parent: CelebornUITab)
  extends WebUIPage("") with Logging {
  import org.apache.spark.shuffle.celeborn.ui.SparkServletBridge._

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
    val assignments = statusStore.assignmentInfos()
    val properties = statusStore.celebornProperties()
    val taskInfo = statusStore.aggregatedTaskInfo()
    val writeTimes = statusStore.writeTimes()
    val perWorkerStats = statusStore.perWorkerWriteStats()
    val readTimes = statusStore.readTimes()
    val perWorkerReadStats = statusStore.perWorkerReadStats()

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
            <strong>Shuffle Write: </strong>
            {
        s"${org.apache.spark.util.Utils.bytesToString(writeBytes)} | Time: ${UIUtils.formatDuration(
          writeMs)} | Speed: ${mbps(writeBytes, writeMs)} MB/s"
      }
          </li>
          <li>
            <strong>Shuffle Read: </strong>
            {
        s"${org.apache.spark.util.Utils.bytesToString(readBytes)} | Time: ${UIUtils.formatDuration(
          readMs)} | Speed: ${mbps(readBytes, readMs)} MB/s"
      }
          </li>
          <li>
            <strong>Shuffle Duration (write+read) / Task Duration: </strong>
            {
        s"${pct(writeMs + readMs, cpuMs)} (Write ${pct(
          writeMs,
          cpuMs)}, Read ${pct(readMs, cpuMs)})"
      }
          </li>
        </ul>
      </div>

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

    val throughputRows: Seq[(String, String)] = Seq(
      (
        "Total Shuffle Write Bytes",
        org.apache.spark.util.Utils.bytesToString(taskInfo.shuffleWriteBytes)),
      (
        "Total Shuffle Write Time",
        org.apache.spark.util.Utils.msDurationToString(taskInfo.shuffleWriteTimeMs)),
      (
        "Total Shuffle Read Bytes",
        org.apache.spark.util.Utils.bytesToString(taskInfo.shuffleReadBytes)),
      (
        "Total Fetch Wait Time",
        org.apache.spark.util.Utils.msDurationToString(taskInfo.shuffleFetchWaitTimeMs)),
      (
        "Total Task CPU Time",
        org.apache.spark.util.Utils.msDurationToString(taskInfo.taskCpuTimeMs)))
    val throughputRowsXml = throughputRows.map { case (label, value) =>
      <tr>
        <td>{label}</td>
        <td>{value}</td>
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

    val writeTimesFields = Seq(
      "Serialize" -> org.apache.spark.util.Utils.msDurationToString(writeTimes.serializeTimeMs),
      "Copy" -> org.apache.spark.util.Utils.msDurationToString(writeTimes.copyTimeMs),
      "Queue Wait" -> org.apache.spark.util.Utils.msDurationToString(writeTimes.queueWaitTimeMs),
      "Background Compress" -> org.apache.spark.util.Utils.msDurationToString(
        writeTimes.compressTimeMs),
      "Background Queue Stall" -> org.apache.spark.util.Utils.msDurationToString(
        writeTimes.queueStallTimeMs),
      "Background Inflight Wait" -> org.apache.spark.util.Utils.msDurationToString(
        writeTimes.inflightWaitTimeMs),
      "Drain Wait" -> org.apache.spark.util.Utils.msDurationToString(writeTimes.drainWaitTimeMs),
      "Max Push RTT" -> org.apache.spark.util.Utils.msDurationToString(writeTimes.maxPushRttMs),
      "Slow Push" -> writeTimes.slowPushCount.toString)
    val writeTimesTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th></th>{writeTimesFields.map { case (n, _) => <th>{n}</th> }}
          </tr>
        </thead>
        <tbody>
          <tr>
            <th>Duration</th>{writeTimesFields.map { case (_, d) => <td>{d}</td> }}
          </tr>
        </tbody>
      </table>

    val perWorkerRows = perWorkerStats.map { s =>
      <tr>
        <td>{s.workerId}</td>
        <td>{org.apache.spark.util.Utils.bytesToString(s.pushBytes)}</td>
        <td>{s.pushCount}</td>
        <td>{
        org.apache.spark.util.Utils.msDurationToString(
          java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(s.totalPushRttNanos))
      }</td>
        <td>{s.softSplitCount}</td>
        <td>{s.hardSplitCount}</td>
        <td>{s.primaryCongestedCount}</td>
        <td>{s.replicaCongestedCount}</td>
        <td>{s.lastPushFailureReason}</td>
      </tr>
    }
    val perWorkerTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>Worker Id</th>
            <th>Push Bytes</th>
            <th>Push Count</th>
            <th>Total Push RTT</th>
            <th>Soft Split</th>
            <th>Hard Split</th>
            <th>Primary Congested</th>
            <th>Replica Congested</th>
            <th>Last Push Failure Reason</th>
          </tr>
        </thead>
        <tbody>
          {perWorkerRows}
        </tbody>
      </table>

    val readTimesFields = Seq(
      "Chunk Wait" -> org.apache.spark.util.Utils.msDurationToString(readTimes.chunkWaitTimeMs),
      "Decompress" -> org.apache.spark.util.Utils.msDurationToString(readTimes.decompressTimeMs),
      "Retry Wait" -> org.apache.spark.util.Utils.msDurationToString(readTimes.retryWaitTimeMs),
      "Max Chunk RTT" -> org.apache.spark.util.Utils.msDurationToString(readTimes.maxChunkRttMs),
      "Slow Chunk" -> readTimes.slowChunkCount.toString)
    val readTimesTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th></th>{readTimesFields.map { case (n, _) => <th>{n}</th> }}
          </tr>
        </thead>
        <tbody>
          <tr>
            <th>Duration</th>{readTimesFields.map { case (_, d) => <td>{d}</td> }}
          </tr>
        </tbody>
      </table>

    val perWorkerReadRows = perWorkerReadStats.map { s =>
      <tr>
        <td>{s.workerId}</td>
        <td>{s.chunkCount}</td>
        <td>{org.apache.spark.util.Utils.bytesToString(s.bytes)}</td>
        <td>{
        org.apache.spark.util.Utils.msDurationToString(
          java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(s.totalRttNanos))
      }</td>
        <td>{
        org.apache.spark.util.Utils.msDurationToString(
          java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(s.maxRttNanos))
      }</td>
      </tr>
    }
    val perWorkerReadTable =
      <table class="table table-bordered table-striped table-sm">
        <thead>
          <tr>
            <th>Worker Id</th>
            <th>Chunk Count</th>
            <th>Read Bytes</th>
            <th>Total RTT</th>
            <th>Max RTT</th>
          </tr>
        </thead>
        <tbody>
          {perWorkerReadRows}
        </tbody>
      </table>

    val content: Seq[Node] =
      <div>
        <span>{summary}</span>
        <script type="text/javascript">{
        scala.xml.Unparsed("""
          if (typeof window.collapseTable !== 'function') {
            window.collapseTable = function(thisName, table) {
              var thisClass = '.' + thisName;
              var tableDiv = $(thisClass).parent().find('.' + table);
              $(tableDiv).toggleClass('collapsed');
              $(thisClass).find('.collapse-table-arrow')
                .toggleClass('arrow-open').toggleClass('arrow-closed');
            };
          }
        """)
      }</script>
        <a name="properties"></a>
        {collapsible("celeborn-properties", "Celeborn Properties", propertiesTable)}
        <a name="throughput"></a>
        {collapsible("throughput", "Shuffle Throughput", throughputTable)}
        <a name="write-times"></a>
        {collapsible("write-times", "Shuffle Write Times", writeTimesTable)}
        <a name="read-times"></a>
        {collapsible("read-times", "Shuffle Read Times", readTimesTable)}
        <a name="write-servers"></a>
        {collapsible("shuffle-write-servers", "Shuffle Write Servers", perWorkerTable)}
        <a name="read-servers"></a>
        {collapsible("shuffle-read-servers", "Shuffle Read Servers", perWorkerReadTable)}
        <a name="assignments"></a>
        {collapsible("assignments", "Shuffle Assignments", assignmentTable)}
      </div>

    UIUtils.headerSparkPage(request, "Celeborn Shuffle Service", content, parent)
  }

  /** A collapsible section (default collapsed), mirroring Uniffle's collapse-table pattern. */
  private def collapsible(id: String, title: String, body: Seq[Node]): Seq[Node] = {
    val spanClass = s"collapse-$id collapse-table"
    val bodyClass = s"$id-table collapsible-table collapsed"
    <div>
      <span class={spanClass} onClick={s"collapseTable('collapse-$id', '$id-table')"}>
        <h4>
          <span class="collapse-table-arrow arrow-closed"></span>
          <a>{title}</a>
        </h4>
      </span>
      <div class={bodyClass}>
        {body}
      </div>
    </div>
  }

  private def propertyHeader: Seq[String] = Seq("Property", "Value")

  private def propertyRow(kv: (String, String)): Seq[Node] =
    <tr>
      <td>{kv._1}</td>
      <td>{kv._2}</td>
    </tr>
}
