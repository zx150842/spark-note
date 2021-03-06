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

package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

import org.apache.spark.annotation.Experimental

private[spark] object StaticSources {
  /**
   * The set of all static sources. These sources may be reported to from any class, including
   * static classes, without requiring reference to a SparkEnv.
   */
  val allSources = Seq(CodegenMetrics)
}

/**
 * :: Experimental ::
 * Metrics for code generation.
 */
@Experimental
object CodegenMetrics extends Source {
  override val sourceName: String = "CodeGenerator"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /**
   * Histogram of the length of source code text compiled by CodeGenerator (in characters).
   */
  val METRIC_SOURCE_CODE_SIZE = metricRegistry.histogram(MetricRegistry.name("sourceCodeSize"))

  /**
   * Histogram of the time it took to compile source code text (in milliseconds).
   */
  val METRIC_COMPILATION_TIME = metricRegistry.histogram(MetricRegistry.name("compilationTime"))
}
