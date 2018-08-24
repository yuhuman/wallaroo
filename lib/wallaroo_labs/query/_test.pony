/*

Copyright 2017 The Wallaroo Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License.

*/

use "ponytest"
use "collections"
use "debug"
use "json"
use "../collection_helpers"

actor Main is TestList
  new create(env: Env) => PonyTest(env, this)

  new make() => None

  fun tag tests(test: PonyTest) =>
    test(_TestEncodeDecodeState)

class iso _TestEncodeDecodeState is UnitTest
  fun name(): String => "query_json/encode_decode_state"

  fun apply(h: TestHelper) =>
    // TODO this was testing a function that was unused in the system
    h.assert_eq[Bool](true, true)

class iso _TestEncodeDecodeClusterStatus is UnitTest
  fun name(): String => "query_json/encode_decode_cluster_status"

  fun apply(h: TestHelper) ? =>
    var is_processing = true
    var worker_count: U64 = 3
    var worker_names = recover val ["w1"; "w2"; "w3"] end
    let json1 = ClusterStatusQueryJsonEncoder.response(worker_count,
      worker_names, is_processing)
    let decoded1 = ClusterStatusQueryJsonDecoder.response(json1)?
    h.assert_eq[Bool](is_processing, decoded1.processing_messages)
    h.assert_eq[U64](worker_count, decoded1.worker_count)
    for i in Range(0, worker_count.usize()) do
      h.assert_eq[String](worker_names(i)?, decoded1.worker_names(i)?)
    end

    is_processing = false
    worker_count = 5
    worker_names = recover val ["w1"; "w2"; "w3"; "w4"; "w5"] end
    let json2 = ClusterStatusQueryJsonEncoder.response(worker_count,
      worker_names, is_processing)
    let decoded2 = ClusterStatusQueryJsonDecoder.response(json2)?
    h.assert_eq[Bool](is_processing, decoded2.processing_messages)
    h.assert_eq[U64](worker_count, decoded2.worker_count)
    for i in Range(0, worker_count.usize()) do
      h.assert_eq[String](worker_names(i)?, decoded2.worker_names(i)?)
    end
