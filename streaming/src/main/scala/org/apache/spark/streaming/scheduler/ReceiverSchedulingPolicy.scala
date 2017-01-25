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

package org.apache.spark.streaming.scheduler

import scala.collection.Map
import scala.collection.mutable

import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.receiver.Receiver

/**
 * A class that tries to schedule receivers with evenly distributed. There are two phases for
 * scheduling receivers.
  *
  * 这个类尝试均匀将receiver调度到集群机器上。调度receiver分为两个阶段
 *
 *  - The first phase is global scheduling when ReceiverTracker is starting and we need to schedule
 *    all receivers at the same time. ReceiverTracker will call `scheduleReceivers` at this phase.
 *    It will try to schedule receivers such that they are evenly distributed. ReceiverTracker
 *    should update its `receiverTrackingInfoMap` according to the results of `scheduleReceivers`.
 *    `ReceiverTrackingInfo.scheduledLocations` for each receiver should be set to a location list
 *    that contains the scheduled locations. Then when a receiver is starting, it will send a
 *    register request and `ReceiverTracker.registerReceiver` will be called. In
 *    `ReceiverTracker.registerReceiver`, if a receiver's scheduled locations is set, it should
 *    check if the location of this receiver is one of the scheduled locations, if not, the register
 *    will be rejected.
  *
  *  第一阶段是全局调度，当ReceiverTracker启动时我们需要同时调度所有的receiver。ReceiverTracker会
  *  在这个阶段调用scheduleReceivers。会尝试将receiver均匀的调度到集群节点执行。ReceiverTracker需
  *  要根据scheduleReceiver方法来更新receiverTrackingInfoMap。每个receiver的ReceiverTrackingInfo.scheduledLocations
  *  需要被设置为包含调度位置的列表。当一个receiver启动时，需要发送注册请求，ReceiverTracker.registerReceiver
  *  会被调用。在Receiver.registerReceiver中，如果receiver的schedule location被设置，则需要检查
  *  receiver的location是否在schedule location中，如果不在，则拒绝注册。
  *
 *  - The second phase is local scheduling when a receiver is restarting. There are two cases of
 *    receiver restarting:
 *    - If a receiver is restarting because it's rejected due to the real location and the scheduled
 *      locations mismatching, in other words, it fails to start in one of the locations that
 *      `scheduleReceivers` suggested, `ReceiverTracker` should firstly choose the executors that
 *      are still alive in the list of scheduled locations, then use them to launch the receiver
 *      job.
 *    - If a receiver is restarting without a scheduled locations list, or the executors in the list
 *      are dead, `ReceiverTracker` should call `rescheduleReceiver`. If so, `ReceiverTracker`
 *      should not set `ReceiverTrackingInfo.scheduledLocations` for this receiver, instead, it
 *      should clear it. Then when this receiver is registering, we can know this is a local
 *      scheduling, and `ReceiverTrackingInfo` should call `rescheduleReceiver` again to check if
 *      the launching location is matching.
  *
  *  第二阶段是当一个receiver重启时的本地调度。在以下两个场景receiver会重启：
  *  - 由于真实注册位置和调度位置不一致，被driver拒绝注册时，receiver需要重启，也就是说receiver在
  *  scheduleReceivers建议的位置启动失败。ReceiverTracker需要首先选择在schedule locations列表中
  *  仍然存活的executor，然后使用这些executor来提交receiver job。
  *  - 如果一个没有schedule locations列表的receiver重启，或者列表中的executor死亡，ReceiverTracker
  *  需要调用rescheduleReceiver。如果是这样的话，ReceiverTracker不能为这个receiver设置ReceiverTrackingInfo.scheduledLocations，
  *  而应该清除这个字段。之后当receiver注册时，我们可以知道这是一个本地调度，ReceiverTrackingInfo
  *  需要再次调用rescheduleReceiver来检查运行位置是否匹配。
  *
 *
 * In conclusion, we should make a global schedule, try to achieve that exactly as long as possible,
 * otherwise do local scheduling.
  *
  * 所以，我们需要一个全局调度，尽可能的使用全局调度，实在不行再使用本地调度
 */
private[streaming] class ReceiverSchedulingPolicy {

  /**
   * Try our best to schedule receivers with evenly distributed. However, if the
   * `preferredLocation`s of receivers are not even, we may not be able to schedule them evenly
   * because we have to respect them.
   *
   * Here is the approach to schedule executors:
   * <ol>
   *   <li>First, schedule all the receivers with preferred locations (hosts), evenly among the
   *       executors running on those host.</li>
   *   <li>Then, schedule all other receivers evenly among all the executors such that overall
   *       distribution over all the receivers is even.</li>
   * </ol>
   *
   * This method is called when we start to launch receivers at the first time.
    *
    * 分配方法如下：
    * 1 如果receiver包含preferred locations，则将receiver均匀调度到这些location上的executor上
    * 2 对于其他receiver，将他们均匀调度到集群所有的executor上
   *
   * @return a map for receivers and their scheduled locations
   */
  def scheduleReceivers(
      receivers: Seq[Receiver[_]],
      executors: Seq[ExecutorCacheTaskLocation]): Map[Int, Seq[TaskLocation]] = {
    if (receivers.isEmpty) {
      return Map.empty
    }

    if (executors.isEmpty) {
      return receivers.map(_.streamId -> Seq.empty).toMap
    }

    // host -> executors on host
    val hostToExecutors = executors.groupBy(_.host)
    // 每个receiver对应的schedule locations数组
    val scheduledLocations = Array.fill(receivers.length)(new mutable.ArrayBuffer[TaskLocation])
    // 每个executor上的receiver数量
    val numReceiversOnExecutor = mutable.HashMap[ExecutorCacheTaskLocation, Int]()
    // Set the initial value to 0
    // 将numReceiversOnExecutor初始化为0
    executors.foreach(e => numReceiversOnExecutor(e) = 0)

    // Firstly, we need to respect "preferredLocation". So if a receiver has "preferredLocation",
    // we need to make sure the "preferredLocation" is in the candidate scheduled executor list.
    // 遍历每个包含preferredLocation的receiver
    for (i <- 0 until receivers.length) {
      // Note: preferredLocation is host but executors are host_executorId
      // 如果receiver设置了preferredLocations
      receivers(i).preferredLocation.foreach { host =>
        hostToExecutors.get(host) match {
          case Some(executorsOnHost) =>
            // preferredLocation is a known host. Select an executor that has the least receivers in
            // this host
            // 如果preferredLocation是有效节点，则选择这个节点上包含最少receiver的executor
            // 找到host上最少receiver的executor
            val leastScheduledExecutor =
              executorsOnHost.minBy(executor => numReceiversOnExecutor(executor))
            // 将executor加入receiver的schedule locations列表
            scheduledLocations(i) += leastScheduledExecutor
            // 更新executor包含的receiver数量
            numReceiversOnExecutor(leastScheduledExecutor) =
              numReceiversOnExecutor(leastScheduledExecutor) + 1
          case None =>
            // preferredLocation is an unknown host.
            // Note: There are two cases:
            // 1. This executor is not up. But it may be up later.
            // 2. This executor is dead, or it's not a host in the cluster.
            // Currently, simply add host to the scheduled executors.

            // Note: host could be `HDFSCacheTaskLocation`, so use `TaskLocation.apply` to handle
            // this case
            scheduledLocations(i) += TaskLocation(host)
        }
      }
    }

    // For those receivers that don't have preferredLocation, make sure we assign at least one
    // executor to them.
    // 遍历不包含preferredLocation的receiver
    for (scheduledLocationsForOneReceiver <- scheduledLocations.filter(_.isEmpty)) {
      // Select the executor that has the least receivers
      // 找到包含receiver最少的executor
      val (leastScheduledExecutor, numReceivers) = numReceiversOnExecutor.minBy(_._2)
      // 将executor更新到scheduledLocations
      scheduledLocationsForOneReceiver += leastScheduledExecutor
      // 更新executor上包含的receiver数量
      numReceiversOnExecutor(leastScheduledExecutor) = numReceivers + 1
    }

    // Assign idle executors to receivers that have less executors
    // 找到所有没有分配receiver的executor
    val idleExecutors = numReceiversOnExecutor.filter(_._2 == 0).map(_._1)
    // 遍历每一个空闲的executor
    for (executor <- idleExecutors) {
      // Assign an idle executor to the receiver that has least candidate executors.
      // 将空闲的executor分配到包含最少executor的receiver中
      val leastScheduledExecutors = scheduledLocations.minBy(_.size)
      leastScheduledExecutors += executor
    }
    // 返回receiver -> scheduleLocations（executor）映射
    receivers.map(_.streamId).zip(scheduledLocations).toMap
  }

  /**
   * Return a list of candidate locations to run the receiver. If the list is empty, the caller can
   * run this receiver in arbitrary executor.
   *
   * This method tries to balance executors' load. Here is the approach to schedule executors
   * for a receiver.
   * <ol>
   *   <li>
   *     If preferredLocation is set, preferredLocation should be one of the candidate locations.
   *   </li>
   *   <li>
   *     Every executor will be assigned to a weight according to the receivers running or
   *     scheduling on it.
   *     <ul>
   *       <li>
   *         If a receiver is running on an executor, it contributes 1.0 to the executor's weight.
   *       </li>
   *       <li>
   *         If a receiver is scheduled to an executor but has not yet run, it contributes
   *         `1.0 / #candidate_executors_of_this_receiver` to the executor's weight.</li>
   *     </ul>
   *     At last, if there are any idle executors (weight = 0), returns all idle executors.
   *     Otherwise, returns the executors that have the minimum weight.
   *   </li>
   * </ol>
   *
   * This method is called when a receiver is registering with ReceiverTracker or is restarting.
    *
    * 返回运行receiver的候选位置列表。如果列表为空，则调用者可以在任意的executor上运行这个receiver。
    * 这个方法尝试平衡每个executor上的负载。具体为一个receiver调度executors方法如下：
    * 1 如果preferredLocation被这是，则preferredLocation需要是候选location之一
    * 2 每个executor根据在其上运行或分配的receiver数量设置权重
    * 2.1 如果一个receiver运行在executor上，则这个receiver贡献的权重为1.0
    * 2.2 如果一个receiver被调度到executor上但还没运行，则贡献的权重为1.0/candidate_executors_of_this_receiver
    * 如果最终还有空闲的executor，则返回全部的空闲executor。否则返回拥有最小权重的executor
   */
  def rescheduleReceiver(
      receiverId: Int,
      preferredLocation: Option[String],
      receiverTrackingInfoMap: Map[Int, ReceiverTrackingInfo],
      executors: Seq[ExecutorCacheTaskLocation]): Seq[TaskLocation] = {
    if (executors.isEmpty) {
      return Seq.empty
    }

    // Always try to schedule to the preferred locations
    val scheduledLocations = mutable.Set[TaskLocation]()
    // Note: preferredLocation could be `HDFSCacheTaskLocation`, so use `TaskLocation.apply` to
    // handle this case
    scheduledLocations ++= preferredLocation.map(TaskLocation(_))

    val executorWeights: Map[ExecutorCacheTaskLocation, Double] = {
      receiverTrackingInfoMap.values.flatMap(convertReceiverTrackingInfoToExecutorWeights)
        .groupBy(_._1).mapValues(_.map(_._2).sum) // Sum weights for each executor
    }

    val idleExecutors = executors.toSet -- executorWeights.keys
    if (idleExecutors.nonEmpty) {
      scheduledLocations ++= idleExecutors
    } else {
      // There is no idle executor. So select all executors that have the minimum weight.
      val sortedExecutors = executorWeights.toSeq.sortBy(_._2)
      if (sortedExecutors.nonEmpty) {
        val minWeight = sortedExecutors(0)._2
        scheduledLocations ++= sortedExecutors.takeWhile(_._2 == minWeight).map(_._1)
      } else {
        // This should not happen since "executors" is not empty
      }
    }
    scheduledLocations.toSeq
  }

  /**
   * This method tries to convert a receiver tracking info to executor weights. Every executor will
   * be assigned to a weight according to the receivers running or scheduling on it:
   *
   * - If a receiver is running on an executor, it contributes 1.0 to the executor's weight.
   * - If a receiver is scheduled to an executor but has not yet run, it contributes
   * `1.0 / #candidate_executors_of_this_receiver` to the executor's weight.
    *
    * 这个方法将receiver tracking info转换为executor权重。每个executor会根据在其上运行或调度到其上的
    * receiver来设置权重：
    * - 一个在executor上运行的receiver贡献1.0权重
    * - 一个调度到executor但还没有运行的receiver贡献1.0/candidate_executors_of_this_receiver权重
   */
  private def convertReceiverTrackingInfoToExecutorWeights(
      receiverTrackingInfo: ReceiverTrackingInfo): Seq[(ExecutorCacheTaskLocation, Double)] = {
    receiverTrackingInfo.state match {
      case ReceiverState.INACTIVE => Nil
      case ReceiverState.SCHEDULED =>
        val scheduledLocations = receiverTrackingInfo.scheduledLocations.get
        // The probability that a scheduled receiver will run in an executor is
        // 1.0 / scheduledLocations.size
        scheduledLocations.filter(_.isInstanceOf[ExecutorCacheTaskLocation]).map { location =>
          location.asInstanceOf[ExecutorCacheTaskLocation] -> (1.0 / scheduledLocations.size)
        }
      case ReceiverState.ACTIVE => Seq(receiverTrackingInfo.runningExecutor.get -> 1.0)
    }
  }
}
