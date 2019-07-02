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

package org.apache.spark.sql.execution.joins

import scala.concurrent.duration._
import scala.util.control.Breaks._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.collection.BitSet


case class MergeAsOfJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftOn: Expression,
    rightOn: Expression,
    leftBy: Expression,
    rightBy: Expression,
    tolerance: String) extends BinaryExecNode {

  override def output: Seq[Attribute] = left.output ++ right.output.map(_.withNullability((true)))

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(Seq(leftBy)) :: HashClusteredDistribution(Seq(rightBy)) :: Nil

  override def outputOrdering: Seq[SortOrder] =
    getKeyOrdering(Seq(leftBy, leftOn), left.outputOrdering)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    Seq(leftBy, leftOn).map(SortOrder(_, Ascending)) ::
      Seq(rightBy, rightOn).map(SortOrder(_, Ascending)) :: Nil
  }

  private val emptyVal: Array[Any] = Array.fill(right.output.length)(null)
  private def rDummy = InternalRow(emptyVal: _*)
  private def joinedRow = new JoinedRow()

  private def getKeyOrdering(keys: Seq[Expression], childOutputOrdering: Seq[SortOrder])
    : Seq[SortOrder] = {
    val requiredOrdering = keys.map(SortOrder(_, Ascending))
    if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
      keys.zip(childOutputOrdering).map { case (key, childOrder) =>
        SortOrder(key, Ascending, childOrder.sameOrderExpressions + childOrder.child - key)
      }
    } else {
      requiredOrdering
    }
  }

  private def match_tolerance(
      currLeft: (InternalRow, Iterator[InternalRow]),
      currRight: (InternalRow, Iterator[InternalRow]),
      tolerance: Duration,
      resultProj: InternalRow => InternalRow
  ): Iterator[InternalRow] = {
//    val joinedRow = new JoinedRow()
    var rHead = if (currRight._2.hasNext) {
      currRight._2.next()
    } else {
      InternalRow.empty
    }
    var rPrev = rHead.copy()

    currLeft._2.map(lHead => {
        breakable {
          while (rHead.getInt(0) <= lHead.getInt(0)) {
            // TODO make index agnostic and check by type (timestamp)
            var rHeadCopy = rHead.copy()
            if (currRight._2.hasNext) {
              rPrev = rHeadCopy.copy()
              rHeadCopy = currRight._2.next()
            } else {
              break
            }
          }
        }
        if (rPrev == InternalRow.empty || rPrev.getInt(0) > lHead.getInt(0)) {
          resultProj(joinedRow(lHead, rDummy))
        } else {
          resultProj(joinedRow(lHead, rPrev))
        }
      }
    )
  }

  protected override def doExecute(): RDD[InternalRow] = {

    val duration = Duration(tolerance)
    val inputSchema = left.output ++ right.output

    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
//      val joinedRow = new JoinedRow()
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, inputSchema)
      if (!leftIter.hasNext || !rightIter.hasNext) {
        leftIter.map(r => resultProj(joinedRow(r, rDummy)))
      } else {
        val joinedRow = new JoinedRow()
        val rightGroupedIterator =
          GroupedIterator(rightIter, Seq(rightBy), right.output)

        if (rightGroupedIterator.hasNext) {
          var currRight = rightGroupedIterator.next()
          val leftGroupedIterator =
            GroupedIterator(leftIter, Seq(leftBy), left.output)
          if (leftGroupedIterator.hasNext) {
            var currLeft = leftGroupedIterator.next()
            match_tolerance(currLeft, currRight, duration, resultProj)
          } else {
            Iterator.empty
          }
        } else {
          Iterator.empty
        }
      }
    }
  }
}
