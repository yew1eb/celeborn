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

    val summary: Seq[Node] =
      <div>
        <ul class="list-unstyled">
          <li>
            <strong>Celeborn Shuffle Service</strong>
          </li>
        </ul>
      </div>

    val buildInfoTable = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true)

    val content =
      <div>
        <span>
          {summary}
        </span>
        <h4>
          <strong>Build Information</strong>
        </h4> ++ buildInfoTable
      </div>

    UIUtils.headerSparkPage(request, "Celeborn Shuffle Service", content, parent)
  }

  private def propertyHeader: Seq[String] = Seq("Property", "Value")

  private def propertyRow(kv: (String, String)): Seq[Node] =
    <td>
      {kv._1}
    </td>
      <td>
        {kv._2}
      </td>
}
