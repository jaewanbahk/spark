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
    exactMatches: Boolean) extends BinaryExecNode {

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

  protected override def doExecute(): RDD[InternalRow] = {
    // Zip the left and right plans to group by key.
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val keyOrdering = newNaturalAscendingOrdering(leftBy.map(_.dataType))
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

      val leftGroupedIterator = GroupedIterator(leftIter, Seq(leftBy), left.output)
      val rightGroupedIterator = GroupedIterator(rightIter, Seq(rightBy), right.output)

      new MergeAsOfIterator(
        leftGroupedIterator,
        rightGroupedIterator,
        resultProj,
        left.output,
        right.output,
        tolerance,
        leftOn,
        rightOn,
        exactMatches,
        keyOrdering
      ).toScala
    }
  }
}


private class MergeAsOfIterator(
  leftGroupedIter: Iterator[(InternalRow, Iterator[InternalRow])],
  rightGroupedIter: Iterator[(InternalRow, Iterator[InternalRow])],
  resultProj: InternalRow => InternalRow,
  leftOutput: Seq[Attribute],
  rightOutput: Seq[Attribute],
  tolerance: Long,
  leftOn: Expression,
  rightOn: Expression,
  exactMatches: Boolean,
  keyOrdering: Ordering[InternalRow]
  ) extends RowIterator {

  private[this] val leftGroupedIterator = leftGroupedIter
  private[this] val rightGroupedIterator = rightGroupedIter

  private[this] val leftOnProj = UnsafeProjection.create(Seq(leftOn), leftOutput)
  private[this] val rightOnProj = UnsafeProjection.create(Seq(rightOn), rightOutput)

  protected[this] val joinedRow: JoinedRow = new JoinedRow()
  private[this] val rightNullRow = new GenericInternalRow(rightOutput.length)
  private[this] var currRight: (InternalRow, Iterator[InternalRow]) = _
  if (rightGroupedIterator.hasNext) currRight = rightGroupedIterator.next()

  private[this] var resIter: Iterator[InternalRow] = _

  override def advanceNext(): Boolean = findNextAsOfJoinRows()

  override def getRow: InternalRow = resIter.next()

  // --- Private methods --------------------------------------------------------------------------

  private def findNextAsOfJoinRows(): Boolean = {
    if (resIter != null && resIter.hasNext) {
      // resIter is either an Iterator returned from match_tolerance or an empty row projection
      true
    } else {
      // resIter empty or exhausted - populate with an iterator
      if (leftGroupedIterator.hasNext) {
        // Should be called once per left group - will always return true
        val currLeft = leftGroupedIterator.next()
        if (currRight == null) {
          // If there is no right group in the same partition, return null projection
          resIter = currLeft._2.map(r => resultProj(joinedRow(r, rightNullRow)))
          return true
        }

        var comp = keyOrdering.compare(currLeft._1, currRight._1)
        if (comp < 0) {
          // Left group key is behind right group key - return null projection
          resIter = currLeft._2.map(r => resultProj(joinedRow(r, rightNullRow)))
        } else if (comp == 0) {
          // Left group key is at right group key - call match tolerance
          resIter = match_tolerance(currLeft, currRight, tolerance, resultProj)
        } else {
          // Left group key is ahead of right group key - catch right group up
          var empty = false
          // While loop to get lagging right row up to speed or until it runs out
          do {
            if (rightGroupedIterator.hasNext) {
              currRight = rightGroupedIterator.next()
            } else {
              empty = true
            }
            comp = keyOrdering.compare(currLeft._1, currRight._1)
          } while (!empty && comp > 0)
          // Breaks out of while loop if the groups are matched or the right runs out
          if (empty || comp < 0) {
            resIter = currLeft._2.map(r => resultProj(joinedRow(r, rightNullRow)))
           } else {
              // If both groups have the same key, proceed with match tolerance
            resIter = match_tolerance(currLeft, currRight, tolerance, resultProj)
          }
        }
        // A nonempty resIter will always be returned (1:1 left rows to output left rows)
        true
      } else {
          // leftIter exhausted; key mismatch due to no more left key groups
        false
      }
    }
  }

  // Helper function performing the join using grouped iterators taking tolerance into account.
  private def match_tolerance(
    currLeft: (InternalRow, Iterator[InternalRow]),
    currRight: (InternalRow, Iterator[InternalRow]),
    tolerance: Long,
    resultProj: InternalRow => InternalRow
  ): Iterator[InternalRow] = {
    // the current groups should be matching and the group should not be empty
    assert(keyOrdering.compare(currLeft._1, currRight._1) == 0)
    assert(currRight._2.hasNext)

    val rHead = currRight._2.next()
    var rPrev = rHead.copy()

    currLeft._2.map(lHead => {
      breakable {
        // Use pointers to determine candidacy of the joining of right rows to left.
        while (exactMatches && rightOnProj(rHead).getLong(0) <= leftOnProj(lHead).getLong(0)
          || !exactMatches && rightOnProj(rHead).getLong(0) < leftOnProj(lHead).getLong(0)) {
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
      val lProj = leftOnProj(lHead).getLong(0)
      val rProj = rightOnProj(rPrev).getLong(0)
      val toleranceCond = tolerance != Long.MaxValue && rProj + tolerance * 1000 < lProj

      if (rPrev == InternalRow.empty ||
        exactMatches && (rProj > lProj || toleranceCond) ||
        !exactMatches && (rProj >= lProj || toleranceCond)) {
        resultProj(joinedRow(lHead, new GenericInternalRow(rightOutput.length)))
      } else {
        resultProj(joinedRow(lHead, rPrev))
      }
    })
  }
}
