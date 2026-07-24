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

package org.apache.celeborn.cli.trogdor

import scala.collection.JavaConverters._

import picocli.CommandLine.{Command, Option}

import org.apache.celeborn.cli.common.BaseCommand
import org.apache.celeborn.trogdor.client.AgentClient

@Command(name = "agent", description = Array("Trogdor agent operations"))
class AgentSubcommand extends BaseCommand {

  @Option(
    names = Array("-t", "--target"),
    description = Array("Agent host:port, e.g. localhost:19090"),
    required = true)
  private var target: String = _

  @Option(
    names = Array("--status"),
    description = Array("Show agent status"))
  private var status: Boolean = false

  @Option(
    names = Array("--uptime"),
    description = Array("Show agent uptime"))
  private var uptime: Boolean = false

  override def run(): Unit = {
    val (host, port) = parseTarget(target)
    val client = new AgentClient(host, port)
    try {
      if (status) logInfo(client.status().workers().asScala.toMap.toString)
      if (uptime) logInfo(client.uptime().toString)
    } finally {
      client.close()
    }
  }

  private def parseTarget(target: String): (String, Int) = {
    val idx = target.lastIndexOf(':')
    require(idx > 0, s"Invalid target: $target")
    (target.substring(0, idx), target.substring(idx + 1).toInt)
  }
}
