// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("any_value") {
    // enable nereids
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    test {
        sql "select any(s_suppkey), any(s_name), any_value(s_address) from supplier;"
    }
    qt_sql_max """select max(cast(concat(number, ":00:00") as time)) from numbers("number" = "100");"""
    qt_sql_min """select min(cast(concat(number, ":00:00") as time)) from numbers("number" = "100");"""
    sql """select any(cast(concat(number, ":00:00") as time)) from numbers("number" = "100");"""
}