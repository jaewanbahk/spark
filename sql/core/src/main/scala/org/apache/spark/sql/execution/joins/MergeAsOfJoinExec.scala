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

import scala.util.control.Breaks._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._

/**
 * Performs an as-of merge join of two DataFrames.
 *
 * This class takes the left and right plans and joins them using a grouped iterator. The "on"
 * value is compared to determine whether that right row will be merged with the left or not.
 */
case class MergeAsOfJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftOn: Expression,
    rightOn: Expression,
    leftBy: Expression,
    rightBy: Expression,
    tolerance: Long,
    allowExactMatches: Boolean) extends BinaryExecNode {

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

  private val joinedRow = new JoinedRow()
  private val emptyVal: Array[Any] = Array.fill(right.output.length)(null)
  private def rDummy = InternalRow(emptyVal: _*)

  /**
    * Utility method to get output ordering for left or right side of the join.
    *
    * Taken from [[SortMergeJoinExec]]
    */
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

  // Helper function performing the join using grouped iterators taking tolerance into account.
  private def match_tolerance(
      currLeft: (InternalRow, Iterator[InternalRow]),
      currRight: (InternalRow, Iterator[InternalRow]),
      tolerance: Long,
      resultProj: InternalRow => InternalRow
  ): Iterator[InternalRow] = {
    var rHead = if (currRight._2.hasNext) {
      currRight._2.next()
    } else {
      InternalRow.empty
    }
    var rPrev = rHead.copy()

    currLeft._2.map(lHead => {
      val leftKeyProj = UnsafeProjection.create(Seq(leftOn), left.output)
      val rightKeyProj = UnsafeProjection.create(Seq(rightOn), right.output)
      breakable {
        // Use pointers to determine candidacy of the joining of right rows to left.
        while (allowExactMatches && rightKeyProj(rHead).getLong(0) <= leftKeyProj(lHead).getLong(0)
          || !allowExactMatches && rightKeyProj(rHead).getLong(0) < leftKeyProj(lHead).getLong(0)) {
          var rHeadCopy = rHead.copy()
          if (currRight._2.hasNext) {
            rPrev = rHeadCopy.copy()
            rHeadCopy = currRight._2.next()
          } else {
            break
          }
        }
      }
      // Obtain the left and right keys of the rows in consideration by projection.
      var lProj = leftKeyProj(lHead).getLong(0)
      var rProj = rightKeyProj(rPrev).getLong(0)
      val toleranceCond = tolerance != Long.MaxValue && rProj + tolerance*1000 < lProj

      if (rPrev == InternalRow.empty ||
        allowExactMatches && (rProj > lProj || toleranceCond) ||
        !allowExactMatches && (rProj >= lProj || toleranceCond)) {
        resultProj(joinedRow(lHead, rDummy))
      } else {
        resultProj(joinedRow(lHead, rPrev))
      }}
    )
  }

  protected override def doExecute(): RDD[InternalRow] = {
    // Zip the left and right plans to group by key.
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)
      if (!leftIter.hasNext || !rightIter.hasNext) {
        leftIter.map(r => resultProj(joinedRow(r, rDummy)))
      } else {
        val rightGroupedIterator =
          GroupedIterator(rightIter, Seq(rightBy), right.output)

        if (rightGroupedIterator.hasNext) {
          var currRight = rightGroupedIterator.next()
          val leftGroupedIterator =
            GroupedIterator(leftIter, Seq(leftBy), left.output)
          if (leftGroupedIterator.hasNext) {
            var currLeft = leftGroupedIterator.next()
            match_tolerance(currLeft, currRight, tolerance, resultProj)
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
