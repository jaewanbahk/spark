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

package org.apache.spark.sql

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.TestUtils.{assertNotSpilled, assertSpilled}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.execution.{BinaryExecNode, SortExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class MergeAsOfSuite extends QueryTest with SharedSQLContext{
  import testImplicits._

  setupTestData()

  def statisticSizeInByte(df: DataFrame): BigInt = {
    df.queryExecution.optimizedPlan.stats.sizeInBytes
  }

  test("basic merge_asof") {
    val df1 = Seq(
      (2001, 1, 1.0),
      (2001, 2, 1.1),
      (2002, 1, 1.2)
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (2001, 1, 4),
      (2001, 2, 5),
    ).toDF("time", "id", "v2")

    val res = df1.mergeAsOf(df2, df1("time"), df2("time"), df1("id"), df2("id"))

    val expected = Seq(
      (2001, 1, 1.0, 4),
      (2002, 1, 1.2, 4),
      (2001, 2, 1.1, 5)
    ).toDF("time", "id", "v", "v2")

    assert(res.collect() === expected.collect())

    val res2 = df1.select("time", "id").mergeAsOf(df2.withColumn("v3", df2("v2") * 3 cast "Int"), df1("time"), df2("time"), df1("id"), df2("id"))

    val expected2 = Seq(
      (2001, 1, 4, 12),
      (2002, 1, 4, 12),
      (2001, 2, 5, 15)
    ).toDF("time", "id", "v2", "v3")

    assert(res2.collect() === expected2.collect())
  }
}
