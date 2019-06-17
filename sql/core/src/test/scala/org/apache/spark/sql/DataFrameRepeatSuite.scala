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

import org.scalatest.concurrent.Eventually

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameRepeatSuite extends QueryTest with SharedSQLContext with Eventually{

  test("SPARK repeat api") {

    val res1 = spark.repeat(1, 5).select("id")
    assert(res1.count == 5)
    assert(res1.agg(sum("id")).as("sumid").collect() === Seq(Row(5)))

    val res2 = spark.repeat(2, 10).select("id")
    assert(res2.count == 10)
    assert(res2.agg(sum("id")).as("sumid").collect() === Seq(Row(20)))

    val res3 = spark.repeat(3, 0).select("id")
    assert(res3.count == 0)

    val res4 = spark.repeat(4, 5, 3).select("id")
    assert(res4.count == 5)
    assert(res4.agg(sum("id")).as("sumid").collect() === Seq(Row(20)))

  }

}
