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

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class MergeAsOfSuite extends QueryTest with SharedSQLContext{
  import testImplicits._

  test("basic merge") {
    val df1 = Seq(
      (new Timestamp(2001), 1, 1.0),
      (new Timestamp(2001), 2, 1.1),
      (new Timestamp(2002), 1, 1.2)
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(2001), 1, 4),
      (new Timestamp(2001), 2, 5),
    ).toDF("time", "id", "v2")

    checkAnswer(
      df1.mergeAsOf(df2, df1("time"), df2("time"), df1("id"), df2("id")),
      Seq(
        Row(new Timestamp(2001), 1, 1.0, 4),
        Row(new Timestamp(2002), 1, 1.2, 4),
        Row(new Timestamp(2001), 2, 1.1, 5)
      ))

    checkAnswer(
      df1.select("time", "id").mergeAsOf(
        df2.withColumn("v3", df2("v2") * 3 cast "Int"), df1("time"), df2("time"), df1("id"), df2("id")
      ),
      Seq(
        Row(new Timestamp(2001), 1, 4, 12),
        Row(new Timestamp(2002), 1, 4, 12),
        Row(new Timestamp(2001), 2, 5, 15)
      ))
  }

  test("default merge_asof") {
    val quotes = Seq(
      (new Timestamp(23), "GOOG", 720.50, 720.93),
      (new Timestamp(23), "MSFT", 51.95, 51.96),
      (new Timestamp(30), "MSFT", 51.97, 51.98),
      (new Timestamp(41), "MSFT", 51.99, 52.00),
      (new Timestamp(48), "GOOG", 720.50, 720.93),
      (new Timestamp(49), "AAPL", 97.99, 98.01),
      (new Timestamp(72), "GOOG", 720.50, 720.88),
      (new Timestamp(75), "MSFT", 52.01, 52.03)
    ).toDF("time", "ticker", "bid", "ask")

    val trades = Seq(
      (new Timestamp(23), "MSFT", 51.95, 75),
      (new Timestamp(38), "MSFT", 51.95, 155),
      (new Timestamp(48), "GOOG", 720.77, 100),
      (new Timestamp(48), "GOOG", 720.92, 100),
      (new Timestamp(48), "AAPL", 98.00, 100)
    ).toDF("time", "ticker", "price", "quantity")

    checkAnswer(
      trades.mergeAsOf(quotes, trades("time"), quotes("time"), trades("ticker"), quotes("ticker")),
      Seq(
        Row(new Timestamp(23), "MSFT", 51.95, 75, 51.95, 51.96),
        Row(new Timestamp(38), "MSFT", 51.95, 155, 51.97, 51.98),
        Row(new Timestamp(48), "GOOG", 720.77, 100, 720.5, 720.93),
        Row(new Timestamp(48), "GOOG", 720.92, 100, 720.5, 720.93),
        Row(new Timestamp(48), "AAPL", 98.0, 100, null, null)
      ))
  }

  test("partial key mismatch") {
    val df1 = Seq(
      (new Timestamp(2001), 1, 1.0),
      (new Timestamp(2001), 2, 1.1),
      (new Timestamp(2002), 1, 1.2)
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(2001), 1, 5),
      (new Timestamp(2001), 4, 4),
    ).toDF("time", "id", "v2")

    checkAnswer(
      df1.mergeAsOf(df2, df1("time"), df2("time"), df1("id"), df2("id")),
      Seq(
        Row(new Timestamp(2001), 1, 1.0, 5),
        Row(new Timestamp(2002), 1, 1.2, 5),
        Row(new Timestamp(2001), 2, 1.1, null)
      ))
  }

  test("complete key mismatch") {
    val df1 = Seq(
      (new Timestamp(2001), 1, 1.0),
      (new Timestamp(2001), 2, 1.1),
      (new Timestamp(2002), 1, 1.2)
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(2001), 3, 5),
      (new Timestamp(2001), 4, 4),
    ).toDF("time", "id", "v2")

    checkAnswer(
      df1.mergeAsOf(df2, df1("time"), df2("time"), df1("id"), df2("id")),
      Seq(
        Row(new Timestamp(2001), 1, 1.0, null),
        Row(new Timestamp(2002), 1, 1.2, null),
        Row(new Timestamp(2001), 2, 1.1, null)
      ))
  }

  test("merge_asof tolerance") {
    val quotes = Seq(
      (new Timestamp(23), "GOOG", 720.50, 720.93),
      (new Timestamp(23), "MSFT", 51.95, 51.96),
      (new Timestamp(30), "MSFT", 51.97, 51.98),
      (new Timestamp(41), "MSFT", 51.99, 52.00),
      (new Timestamp(48), "GOOG", 720.50, 720.93),
      (new Timestamp(49), "AAPL", 97.99, 98.01),
      (new Timestamp(72), "GOOG", 720.50, 720.88),
      (new Timestamp(75), "MSFT", 52.01, 52.03)
    ).toDF("time", "ticker", "bid", "ask")

    val trades = Seq(
      (new Timestamp(23), "MSFT", 51.95, 75),
      (new Timestamp(38), "MSFT", 51.95, 155),
      (new Timestamp(48), "GOOG", 720.77, 100),
      (new Timestamp(48), "GOOG", 720.92, 100),
      (new Timestamp(48), "AAPL", 98.00, 100)
    ).toDF("time", "ticker", "price", "quantity")

    checkAnswer(
      trades.mergeAsOf(quotes, trades("time"), quotes("time"), trades("ticker"), quotes("ticker"), "2ms"),
      Seq(
        Row(new Timestamp(23), "MSFT", 51.95, 75, 51.95, 51.96),
        Row(new Timestamp(38), "MSFT", 51.95, 155, null, null),
        Row(new Timestamp(48), "GOOG", 720.77, 100, 720.5, 720.93),
        Row(new Timestamp(48), "GOOG", 720.92, 100, 720.5, 720.93),
        Row(new Timestamp(48), "AAPL", 98.0, 100, null, null)
      ))
  }

  test("merge_asof tolerance allow exact matches") {
    val quotes = Seq(
      (new Timestamp(23), "GOOG", 720.50, 720.93),
      (new Timestamp(23), "MSFT", 51.95, 51.96),
      (new Timestamp(30), "MSFT", 51.97, 51.98),
      (new Timestamp(41), "MSFT", 51.99, 52.00),
      (new Timestamp(48), "GOOG", 720.50, 720.93),
      (new Timestamp(49), "AAPL", 97.99, 98.01),
      (new Timestamp(72), "GOOG", 720.50, 720.88),
      (new Timestamp(75), "MSFT", 52.01, 52.03)
    ).toDF("time", "ticker", "bid", "ask")

    val trades = Seq(
      (new Timestamp(23), "MSFT", 51.95, 75),
      (new Timestamp(38), "MSFT", 51.95, 155),
      (new Timestamp(48), "GOOG", 720.77, 100),
      (new Timestamp(48), "GOOG", 720.92, 100),
      (new Timestamp(48), "AAPL", 98.00, 100)
    ).toDF("time", "ticker", "price", "quantity")

    checkAnswer(
      trades.mergeAsOf(quotes, trades("time"), quotes("time"), trades("ticker"), quotes("ticker"), "10ms", false),
      Seq(
        Row(new Timestamp(23), "MSFT", 51.95, 75, null, null),
        Row(new Timestamp(38), "MSFT", 51.95, 155, 51.97, 51.98),
        Row(new Timestamp(48), "GOOG", 720.77, 100, null, null),
        Row(new Timestamp(48), "GOOG", 720.92, 100, null, null),
        Row(new Timestamp(48), "AAPL", 98.0, 100, null, null)
      ))
  }
}
