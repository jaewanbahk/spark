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

import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

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

  test("self asof on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(114), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(116), 17, "a")
    ).toDF("time", "id", "v")

    checkAnswer(
      df.mergeAsOf(df, df("time"), df("time"), df("id"), df("id")),
      Seq(
        Row(new Timestamp(100), 1, "a", "a"),
        Row(new Timestamp(117), 1, "b", "b"),
        Row(new Timestamp(118), 1, "c", "c"),
        Row(new Timestamp(119), 1, "d", "d"),
        Row(new Timestamp(101), 2, "a", "a"),
        Row(new Timestamp(102), 3, "a", "a"),
        Row(new Timestamp(103), 4, "a", "a"),
        Row(new Timestamp(104), 5, "a", "a"),
        Row(new Timestamp(105), 6, "a", "a"),
        Row(new Timestamp(120), 6, "b", "b"),
        Row(new Timestamp(121), 6, "c", "c"),
        Row(new Timestamp(106), 7, "a", "a"),
        Row(new Timestamp(107), 8, "a", "a"),
        Row(new Timestamp(108), 9, "a", "a"),
        Row(new Timestamp(109), 10, "a", "a"),
        Row(new Timestamp(110), 11, "a", "a"),
        Row(new Timestamp(122), 11, "b", "b"),
        Row(new Timestamp(111), 12, "a", "a"),
        Row(new Timestamp(112), 13, "a", "a"),
        Row(new Timestamp(113), 14, "a", "a"),
        Row(new Timestamp(114), 15, "a", "a"),
        Row(new Timestamp(115), 16, "a", "a"),
        Row(new Timestamp(116), 17, "a", "a")
      ))
  }

  test("asof on left superset on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(114), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(116), 17, "a")
    ).toDF("time", "id", "v")

    val df1 = Seq(
      (new Timestamp(100), 1, "b"),
      (new Timestamp(101), 3, "b"),
      (new Timestamp(102), 5, "b"),
      (new Timestamp(103), 7, "b"),
      (new Timestamp(104), 9, "b"),
      (new Timestamp(105), 11, "b")
    ).toDF("time", "id", "v2")

    checkAnswer(
      df.mergeAsOf(df1, df("time"), df1("time"), df("id"), df1("id")),
      Seq(
        Row(new Timestamp(100), 1, "a", "b"),
        Row(new Timestamp(117), 1, "b", "b"),
        Row(new Timestamp(118), 1, "c", "b"),
        Row(new Timestamp(119), 1, "d", "b"),
        Row(new Timestamp(101), 2, "a", null),
        Row(new Timestamp(102), 3, "a", "b"),
        Row(new Timestamp(103), 4, "a", null),
        Row(new Timestamp(104), 5, "a", "b"),
        Row(new Timestamp(105), 6, "a", null),
        Row(new Timestamp(120), 6, "b", null),
        Row(new Timestamp(121), 6, "c", null),
        Row(new Timestamp(106), 7, "a", "b"),
        Row(new Timestamp(107), 8, "a", null),
        Row(new Timestamp(108), 9, "a", "b"),
        Row(new Timestamp(109), 10, "a", null),
        Row(new Timestamp(110), 11, "a", "b"),
        Row(new Timestamp(122), 11, "b", "b"),
        Row(new Timestamp(111), 12, "a", null),
        Row(new Timestamp(112), 13, "a", null),
        Row(new Timestamp(113), 14, "a", null),
        Row(new Timestamp(114), 15, "a", null),
        Row(new Timestamp(115), 16, "a", null),
        Row(new Timestamp(116), 17, "a", null)
      ))
  }

  test("asof on right superset on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(114), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(116), 17, "a")
    ).toDF("time", "id", "v")

    val df1 = Seq(
      (new Timestamp(100), 1, "b"),
      (new Timestamp(101), 3, "b"),
      (new Timestamp(102), 5, "b"),
      (new Timestamp(103), 7, "b"),
      (new Timestamp(104), 9, "b"),
      (new Timestamp(105), 11, "b")
    ).toDF("time", "id", "v2")

    checkAnswer(
      df1.mergeAsOf(df, df1("time"), df("time"), df1("id"), df("id")),
      Seq(
        Row(new Timestamp(100), 1, "b", "a"),
        Row(new Timestamp(101), 3, "b", null),
        Row(new Timestamp(102), 5, "b", null),
        Row(new Timestamp(103), 7, "b", null),
        Row(new Timestamp(104), 9, "b", null),
        Row(new Timestamp(105), 11, "b", null)
      ))
  }

  test("asof on left intersect on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(114), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(116), 17, "a")
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(100), 15, "c"),
      (new Timestamp(101), 16, "c"),
      (new Timestamp(102), 17, "c"),
      (new Timestamp(103), 18, "c"),
      (new Timestamp(104), 19, "c"),
      (new Timestamp(105), 20, "c")
    ).toDF("time", "id", "v3")

    checkAnswer(
      df.mergeAsOf(df2, df("time"), df2("time"), df("id"), df2("id")),
      Seq(
        Row(new Timestamp(100), 1, "a", null),
        Row(new Timestamp(117), 1, "b", null),
        Row(new Timestamp(118), 1, "c", null),
        Row(new Timestamp(119), 1, "d", null),
        Row(new Timestamp(101), 2, "a", null),
        Row(new Timestamp(102), 3, "a", null),
        Row(new Timestamp(103), 4, "a", null),
        Row(new Timestamp(104), 5, "a", null),
        Row(new Timestamp(105), 6, "a", null),
        Row(new Timestamp(120), 6, "b", null),
        Row(new Timestamp(121), 6, "c", null),
        Row(new Timestamp(106), 7, "a", null),
        Row(new Timestamp(107), 8, "a", null),
        Row(new Timestamp(108), 9, "a", null),
        Row(new Timestamp(109), 10, "a", null),
        Row(new Timestamp(110), 11, "a", null),
        Row(new Timestamp(122), 11, "b", null),
        Row(new Timestamp(111), 12, "a", null),
        Row(new Timestamp(112), 13, "a", null),
        Row(new Timestamp(113), 14, "a", null),
        Row(new Timestamp(114), 15, "a", "c"),
        Row(new Timestamp(115), 16, "a", "c"),
        Row(new Timestamp(116), 17, "a", "c")
      ))
  }


  test("asof on right intersect on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(99), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(100), 17, "a")
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(100), 15, "c"),
      (new Timestamp(101), 16, "c"),
      (new Timestamp(102), 17, "c"),
      (new Timestamp(103), 18, "c"),
      (new Timestamp(104), 19, "c"),
      (new Timestamp(105), 20, "c")
    ).toDF("time", "id", "v3")

    checkAnswer(
      df2.mergeAsOf(df, df2("time"), df("time"), df2("id"), df("id")),
      Seq(
        Row(new Timestamp(100), 15, "c", "a"),
        Row(new Timestamp(101), 16, "c", null),
        Row(new Timestamp(102), 17, "c", "a"),
        Row(new Timestamp(103), 18, "c", null),
        Row(new Timestamp(104), 19, "c", null),
        Row(new Timestamp(105), 20, "c", null)
      ))
  }
}
