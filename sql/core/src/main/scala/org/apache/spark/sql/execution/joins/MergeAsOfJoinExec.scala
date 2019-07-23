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

  private def joinedRow: JoinedRow = new JoinedRow()
  private def rightNullRow: GenericInternalRow = new GenericInternalRow(right.output.length)
  private def keyOrdering: Ordering[InternalRow] =
    newNaturalAscendingOrdering(leftBy.map(_.dataType))

  protected override def doExecute(): RDD[InternalRow] = {

    // Zip the left and right plans to group by key.
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

      val maoScanner = new MergeAsOfScanner(
        leftIter,
        rightIter,
        leftOn,
        rightOn,
        leftBy,
        rightBy,
        left.output,
        right.output
      )

      new MergeAsOfIterator(
        maoScanner, resultProj, tolerance, exactMatches, keyOrdering, joinedRow, rightNullRow
      ).toScala
    }
  }
}

private class MergeAsOfScanner(
    leftIter: Iterator[InternalRow],
    rightIter: Iterator[InternalRow],
    leftOn: Expression,
    rightOn: Expression,
    leftBy: Expression,
    rightBy: Expression,
    leftOutput: Seq[Attribute],
    rightOutput: Seq[Attribute]) {

  def getLeftGroupedIterator: Iterator[(InternalRow, Iterator[InternalRow])] =
    GroupedIterator(leftIter, Seq(leftBy), leftOutput)
  def getRightGroupedIterator: Iterator[(InternalRow, Iterator[InternalRow])] =
    GroupedIterator(rightIter, Seq(rightBy), rightOutput)
  def getLeftProj: UnsafeProjection = UnsafeProjection.create(Seq(leftOn), leftOutput)
  def getRightProj: UnsafeProjection = UnsafeProjection.create(Seq(rightOn), rightOutput)
}


private class MergeAsOfIterator(
    maoScanner: MergeAsOfScanner,
    resultProj: InternalRow => InternalRow,
    tolerance: Long,
    exactMatches: Boolean,
    keyOrdering: Ordering[InternalRow],
    joinRow: JoinedRow,
    rNullRow: GenericInternalRow
  ) extends RowIterator {

  private[this] val leftGroupedIterator = maoScanner.getLeftGroupedIterator
  private[this] val rightGroupedIterator = maoScanner.getRightGroupedIterator

  private[this] val leftOnProj = maoScanner.getLeftProj
  private[this] val rightOnProj = maoScanner.getRightProj

  protected[this] val joinedRow: JoinedRow = joinRow
  private[this] val rightNullRow = rNullRow
  private[this] var currRight: (InternalRow, Iterator[InternalRow]) = _
  if (rightGroupedIterator.hasNext) currRight = rightGroupedIterator.next()

  // Iterator container populated with matched rows or an empty row projection
  private[this] var resIter: Iterator[InternalRow] = _

  override def advanceNext(): Boolean = findNextAsOfJoinRows()

  override def getRow: InternalRow = resIter.next()

  // --- Private methods --------------------------------------------------------------------------

  private def findNextAsOfJoinRows(): Boolean = {
    if (resIter != null && resIter.hasNext) {
      true
    } else {
      // resIter empty or exhausted - populate with an iterator
      if (leftGroupedIterator.hasNext) {
        // Called once per left group - will always return true
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
          resIter = match_tolerance(currLeft._2, currRight._2, tolerance, resultProj)
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
          if (empty || comp < 0) {
            resIter = currLeft._2.map(r => resultProj(joinedRow(r, rightNullRow)))
           } else {
            resIter = match_tolerance(currLeft._2, currRight._2, tolerance, resultProj)
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
    currLeftIter: Iterator[InternalRow],
    currRightIter: Iterator[InternalRow],
    tolerance: Long,
    resultProj: InternalRow => InternalRow
  ): Iterator[InternalRow] = {
    // The current groups should be matching and the group should not be empty.
    assert(currRightIter.hasNext)
    var rHead = currRightIter.next()
    var rPrev = rHead.copy()

    currLeftIter.map(lHead => {
      breakable {
        // Use pointers to determine candidacy of the joining of right rows to left.
        while (exactMatches && rightOnProj(rHead).getLong(0) <= leftOnProj(lHead).getLong(0)
          || !exactMatches && rightOnProj(rHead).getLong(0) < leftOnProj(lHead).getLong(0)) {
          rPrev = rHead.copy()
          if (currRightIter.hasNext) {
            rHead = currRightIter.next()
          } else {
            break
          }
        }
      }

      // Obtain the left and right keys of the rows in consideration by projection.
      val lProj = leftOnProj(lHead).getLong(0)
      val rProj = rightOnProj(rPrev).getLong(0)
      val toleranceCond = tolerance != Long.MaxValue && rProj + tolerance * 1000 < lProj

      if (exactMatches && (rProj > lProj || toleranceCond) ||
        !exactMatches && (rProj >= lProj || toleranceCond)) {
        resultProj(joinedRow(lHead, rightNullRow))
      } else {
        resultProj(joinedRow(lHead, rPrev))
      }
    })
  }
}
