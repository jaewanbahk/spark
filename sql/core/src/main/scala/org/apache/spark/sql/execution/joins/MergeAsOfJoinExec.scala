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

  private val joinedRow = new JoinedRow()

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

      new MergeAsOfIterator(
        leftIter,
        rightIter,
        resultProj,
        leftBy,
        rightBy,
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
  leftIter: Iterator[InternalRow],
  rightIter: Iterator[InternalRow],
  resultProj: InternalRow => InternalRow,
  leftBy: Expression,
  rightBy: Expression,
  leftOutput: Seq[Attribute],
  rightOutput: Seq[Attribute],
  tolerance: Long,
  leftOn: Expression,
  rightOn: Expression,
  exactMatches: Boolean,
  keyOrdering: Ordering[InternalRow]
  ) extends RowIterator {

  protected[this] val joinedRow: JoinedRow = new JoinedRow()
  private[this] val rightNullRow = new GenericInternalRow(rightOutput.length)

  private[this] val leftGroupedIterator = GroupedIterator(leftIter, Seq(leftBy), leftOutput)
  private[this] val rightGroupedIterator = GroupedIterator(rightIter, Seq(rightBy), rightOutput)

  private[this] var currLeft: (InternalRow, Iterator[InternalRow]) = _
  private[this] var currRight: (InternalRow, Iterator[InternalRow]) = _

  private[this] var resIter: Iterator[InternalRow] = _

  override def advanceNext(): Boolean = {
    if (findNextAsOfJoinRows()) {
      true
    } else {
      false
    }
  }

  override def getRow: InternalRow = resIter.next()

  // --- Private methods --------------------------------------------------------------------------

  private def findNextAsOfJoinRows(): Boolean = {

    if (resIter != null && resIter.hasNext) {
      // resIter is either an Iterator returned from match_tolerance or an empty row projection
      true
    } else {
      // resIter has exhausted its iterations; look for next iterator
      if (!rightGroupedIterator.hasNext) {
        // Consumed the entire right iterator
        if (leftGroupedIterator.hasNext) {
          resIter = leftGroupedIterator.next()._2.map(r => resultProj(joinedRow(r, rightNullRow)))
          // There are groups in the left DF that are not in the right DF; return null projection
          true
        } else {
          // There are no more groups in left or right DF in this partition; terminate
          false
        }
      } else {
        if (leftGroupedIterator.hasNext) {
          currLeft = leftGroupedIterator.next()
          currRight = rightGroupedIterator.next()

          if (keyOrdering.compare(currLeft._1, currRight._1) == 0) {
            // Matches current group, so call match_tolerance
            resIter = match_tolerance(currLeft, currRight, tolerance, resultProj)
            true
          } else {
            // There is a mismatch in the current group keys
            // - either the right is ahead or left is ahead in terms of group ordering

            // Because this is an As-Of Join, the left rows will always be returned so we assume
            // the right rows are lagging and get the right rows up to speed (comp > 0)
            var comp = 1
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
            // Breaks out of while loop if comp = 0 (the groups are matched; ideal) or
            // comp < 0 (the right exceeds the left) [or the right runs out]
            if (comp == 0) {
              resIter = match_tolerance(currLeft, currRight, tolerance, resultProj)
              // If both groups have the same key, proceed with the match tolerance
              true
            } else {
              // If the right row is empty or past the left group, then we ignore it
              false
            }
          }
        } else {
          // Key mismatch due to right keys that do not have a corresponding left key
          false
        }
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
      var rHead = if (currRight._2.hasNext) {
        currRight._2.next()
      } else {
        InternalRow.empty
      }
      var rPrev = rHead.copy()

      currLeft._2.map(lHead => {
        val leftKeyProj = UnsafeProjection.create(Seq(leftOn), leftOutput)
        val rightKeyProj = UnsafeProjection.create(Seq(rightOn), rightOutput)
        breakable {
          // Use pointers to determine candidacy of the joining of right rows to left.
          while (exactMatches && rightKeyProj(rHead).getLong(0) <= leftKeyProj(lHead).getLong(0)
            || !exactMatches && rightKeyProj(rHead).getLong(0) < leftKeyProj(lHead).getLong(0)) {
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
        val toleranceCond = tolerance != Long.MaxValue && rProj + tolerance * 1000 < lProj

        if (rPrev == InternalRow.empty ||
          exactMatches && (rProj > lProj || toleranceCond) ||
          !exactMatches && (rProj >= lProj || toleranceCond)) {
          resultProj(joinedRow(lHead, new GenericInternalRow(rightOutput.length)))
        } else {
          resultProj(joinedRow(lHead, rPrev))
        }
      }
    )
  }
}
