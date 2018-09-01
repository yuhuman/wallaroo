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

use "../collection_helpers"
use "collections"
use "debug"
use "itertools"
use "json"
use "ponycheck"
use "ponytest"

actor Main is TestList
  new create(env: Env) => PonyTest(env, this)

  new make() => None

  fun tag tests(test: PonyTest) =>
    test(_TestEncodeDecodeClusterStatus)
    test(Property1UnitTest[Array[String]](_SourceIdsCodecProperty))
    test(Property1UnitTest[Map[String,Array[String]]](
      _StateEntityEncodingProperty))
    test(Property1UnitTest[StateAndStatelessDigest](
      _PartitionQueryIdsEncodingProperty))


class _SourceIdsCodecProperty is Property1[Array[String]]
  fun name(): String => "query_json/source_ids_codec_prop"

  fun gen() : Generator[Array[String]] => GenArrayString()

  fun property(arg1: Array[String], ph: PropertyHelper) ? =>
    let encoded = SourceIdsQueryEncoder.response(ArrayToVal(arg1))
    let response = SourceIdsQueryJsonDecoder.response(encoded)?

    ph.assert_true(JsonEq.parsed(encoded, response.json)?)
    ph.assert_array_eq[String](arg1, response.source_ids)


class _StateEntityEncodingProperty is Property1[Map[String,Array[String]]]
  fun name(): String => "query_json/state_entity_encoding_prop"

  fun gen() : Generator[Map[String,Array[String]]] =>
    GenMapStringToArrayString()

  fun property(digest: Map[String,Array[String]], ph: PropertyHelper) ? =>
    let encoded: String =
      StateEntityQueryEncoder.state_entity_keys(MapToVal(digest))
    let doc = JsonDoc
    doc.parse(encoded) ?
    ph.assert_isnt[JsonType](None, doc.data)

type StateAndStatelessDigest is
  Map[String,Map[String,Map[String,Array[String]]]]

class _PartitionQueryIdsEncodingProperty
  is Property1[StateAndStatelessDigest]

  fun name() : String => "query_json/partition_query_ids_prop"

  fun gen() : Generator[StateAndStatelessDigest] =>
    Generator[StateAndStatelessDigest](
    object is GenObj[StateAndStatelessDigest]
      fun generate(rnd: Randomness): GenerateResult[StateAndStatelessDigest] ?
      =>
      let st_ent : Map[String,Map[String,Array[String]]] =
        GenMapStringToMapStringToArrayString().generate_value(rnd)?
      let sl_ent : Map[String,Map[String,Array[String]]] =
        GenMapStringToMapStringToArrayString().generate_value(rnd)?
      let digest : StateAndStatelessDigest =
         Map[String,Map[String,Map[String,Array[String]]]]()
      digest.update("state_partitions", consume st_ent)
      digest.update("stateless_partitions", consume sl_ent)
      digest
     end)

  fun property(digest: StateAndStatelessDigest, ph: PropertyHelper) ?
  =>
    let digest' = MapMapMapToVal(digest)
    let encoded = PartitionQueryStateAndStatelessIdsEncoder(digest')
    let doc = JsonDoc
    doc.parse(encoded) ?
    ph.assert_isnt[JsonType](None, doc.data)

class iso _TestEncodeDecodeClusterStatus is UnitTest
  fun name(): String => "query_json/encode_decode_cluster_status"

  fun apply(h: TestHelper) ? =>
    var stop_the_world_in_process = false
    var is_processing = not stop_the_world_in_process
    var worker_count: U64 = 3
    var worker_names = recover val ["w1"; "w2"; "w3"] end
    let json1 = ClusterStatusQueryJsonEncoder.response(worker_count,
      worker_names, stop_the_world_in_process)
    let decoded1 = ClusterStatusQueryJsonDecoder.response(json1)?
    h.assert_eq[Bool](is_processing, decoded1.processing_messages)
    h.assert_eq[U64](worker_count, decoded1.worker_count)
    for i in Range(0, worker_count.usize()) do
      h.assert_eq[String](worker_names(i)?, decoded1.worker_names(i)?)
    end

    stop_the_world_in_process = true
    is_processing = not stop_the_world_in_process
    worker_count = 5
    worker_names = recover val ["w1"; "w2"; "w3"; "w4"; "w5"] end
    let json2 = ClusterStatusQueryJsonEncoder.response(worker_count,
      worker_names, stop_the_world_in_process)
    let decoded2 = ClusterStatusQueryJsonDecoder.response(json2)?
    h.assert_eq[Bool](is_processing, decoded2.processing_messages)
    h.assert_eq[U64](worker_count, decoded2.worker_count)
    for i in Range(0, worker_count.usize()) do
      h.assert_eq[String](worker_names(i)?, decoded2.worker_names(i)?)
    end


primitive JsonEq
  fun parsed(s: String, t: String): Bool ? =>
    let s' = JsonDoc
    let t' = JsonDoc
    s'.parse(s) ?
    t'.parse(t) ?
    JsonEq(s'.data, t'.data)

  fun apply(v1: JsonType, v2: JsonType) : Bool =>
    match (v1,v2)
    | (None, None) => true
    | (let s: F64, let t: F64) => s == t
    | (let s: I64, let t: I64) => s == t
    | (let s: Bool, let t: Bool) => s == t
    | (let s: String, let t: String) => s == t
    | (let s: JsonArray, let t: JsonArray) =>
       (s.data.size() == t.data.size()) and
       Iter[JsonType](s.data.values())
         .zip[JsonType](t.data.values())
        .all({(xy) => JsonEq(xy._1, xy._2)})
    | (let s: JsonObject, let t: JsonObject) =>
      _equal_keys(s, t) and _all_s_vals_equal_in_t(s,t)
    else
      false
    end

  fun _equal_keys(s: JsonObject, t: JsonObject) : Bool =>
    let skeys: Set[String] =
      Iter[String](s.data.keys())
      .fold[Set[String]](Set[String], {(s, el) => s.add(el)})
    let tkeys: Set[String] =
      Iter[String](t.data.keys())
      .fold[Set[String]](Set[String], {(s, el) => s.add(el)})
    skeys == tkeys

  fun _all_s_vals_equal_in_t(s: JsonObject, t: JsonObject) : Bool =>
    var res = true
    for (s_key, s_val) in s.data.pairs() do
      try
        if not JsonEq(s_val, t.data(s_key)?) then res = false; break end
      else // key doesn't exist in t
        res = false; break
      end
    end
    res


primitive GenArrayString
  fun apply() : Generator[Array[String]] =>
    Generators.array_of[String](Generators.ascii_letters() where max=10)


primitive GenMapStringToArrayString
  fun apply() : Generator[Map[String,Array[String]]] =>
    let tup: Generator[(String, Array[String])] =
      Generators.zip2[String, Array[String]](Generators.ascii_letters(1,32),
                                             GenArrayString())
    Generators.map_of[String, Array[String]](tup where max=10)


primitive GenMapStringToMapStringToArrayString
  fun apply() : Generator[Map[String,Map[String,Array[String]]]] =>
    let tup: Generator[(String, Map[String,Array[String]])] =
      Generators.zip2[String, Map[String,Array[String]]](
        Generators.ascii_letters(1,32),
        GenMapStringToArrayString())
    Generators.map_of[String, Map[String,Array[String]]](tup where max=10)

primitive ArrayToVal
  fun apply(a: Array[String] ref) : Array[String] val =>
    let result: Array[String] trn = recover trn Array[String] end
    for v in a.values() do result.push(v) end
    consume result

primitive MapToVal
  fun apply(m: Map[String, Array[String] ref] ref)
    : Map[String, Array[String] val] val
  =>
    let result: Map[String,Array[String] val] trn =
      recover trn Map[String,Array[String] val] end
    for (k,v) in m.pairs() do result(k) = ArrayToVal(v) end
    consume result

primitive MapMapToVal
  fun apply(m: Map[String,Map[String, Array[String] ref] ref] ref)
    : Map[String,Map[String, Array[String] val] val] val
  =>
    let result: Map[String,Map[String,Array[String] val] val] trn =
      recover trn Map[String,Map[String,Array[String] val] val] end
    for (k,v) in m.pairs() do
      result(k) = MapToVal(v) end
    consume result

primitive MapMapMapToVal
  fun apply(m: Map[String,
                   Map[String,Map[String, Array[String] ref] ref] ref] ref)
    : Map[String,
          Map[String,Map[String, Array[String] val] val] val] val
  =>
    let result: Map[String,
                    Map[String,Map[String,Array[String] val] val] val] trn =
      recover trn
        Map[String,Map[String,Map[String,Array[String] val] val] val]
      end
    for (k,v) in m.pairs() do
      result(k) = MapMapToVal(v) end
    consume result
