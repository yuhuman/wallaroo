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

use "collections"
use "itertools"
use "json"
use "../messages"
use "../../wallaroo/core/common"

type JsonDocUnion is (F64 val | I64 val | Bool val
                     | None val | String val
                     | JsonArray ref | JsonObject ref)
type _StepIds is Array[String] val
type _StepIdsByWorker is Map[String, _StepIds] val
type _PartitionQueryMap is Map[String, _StepIdsByWorker] val


primitive PartitionQueryEncoder
  fun _encode(
    m: Map[String, _PartitionQueryMap],
    f: {(_StepIds): JsonDocUnion}) :
    String
  =>
    let top = JsonObject
    for (category, pm) in m.pairs() do
      let cat = JsonObject
      for (app, worker_map) in pm.pairs() do
        let app_workers = JsonObject
        for (worker, parts) in worker_map.pairs() do
          app_workers.data(worker) = f(parts)
        end
        cat.data(app) = app_workers
      end
      top.data(category) = cat
    end
    top.string()

  fun state_and_stateless(m: Map[String, _PartitionQueryMap]): String =>
    _encode(m, {(parts) =>
                  let arr = JsonArray
                  for part in parts.values() do arr.data.push(part) end
                  arr})

  fun state_and_stateless_by_count(m: Map[String, _PartitionQueryMap]):
      String
  =>
    _encode(m, {(parts) => I64.from[USize](parts.size())})


primitive StateEntityQueryEncoder
  fun state_entity_keys(
    digest: Map[String, Array[String] val] val):
    String
  =>
    let o = JsonObject
    for (k,vs) in digest.pairs() do
      let a = JsonArray
      for v in vs.values() do a.data.push(v) end
      o.data(k) = a
    end
    o.string()


primitive StateEntityCountQueryEncoder
  fun state_entity_count(
    digest: Map[String, Array[String] val] val):
    String
  =>
    let o = JsonObject
    for (k,v) in digest.pairs() do o.data(k) = I64.from[USize](v.size()) end
    o.string()


primitive StatelessPartitionQueryEncoder
  fun stateless_partition_keys(
    stateless_partitions: Map[String, Map[String, Array[String] val] val] val):
    String
  =>
    let o = JsonObject
    for (key,worker_parts) in stateless_partitions.pairs() do
      let o' = JsonObject
      for (worker, parts) in worker_parts.pairs() do
        let a = JsonArray
        for part in parts.values() do a.data.push(part) end
        o'.data(worker) = a
      end
      o.data(key) = o'
    end
    o.string()


primitive StatelessPartitionCountQueryEncoder
  fun stateless_partition_count(
    stateless_partitions: Map[String, Map[String, Array[String] val] val] val):
    String
  =>
    let o = JsonObject
    for (key,worker_parts) in stateless_partitions.pairs() do
      let o' = JsonObject
      for (worker, parts) in worker_parts.pairs() do
        o'.data(worker) = I64.from[USize](parts.size())
      end
      o.data(key) = o'
    end
    o.string()


primitive ShrinkQueryJsonEncoder
  fun request(query: Bool, node_names: Array[String] val, node_count: U64):
    String
  =>
    let o = JsonObject
    let names = JsonArray
    for n in node_names.values() do names.data.push(n) end
    o.data("query") = query
    o.data("node_names") = names
    o.data("node_count") = I64.from[U64](node_count)
    o.string()

  fun response(node_names: Array[String] val, node_count: U64): String =>
    let o = JsonObject
    let names = JsonArray
    for n in node_names.values() do names.data.push(n) end
    o.data("node_names") = names
    o.data("node_count") = I64.from[U64](node_count)
    o.string()


primitive ClusterStatusQueryJsonEncoder
  fun response(worker_count: U64, worker_names: Array[String] val,
    stop_the_world_in_process: Bool): String
  =>
    let o = JsonObject
    let names = JsonArray
    for n in worker_names.values() do names.data.push(n) end
    o.data("worker_count") = I64.from[U64](worker_count)
    o.data("worker_names") = names
    o.data("processing_messages") = not stop_the_world_in_process
    o.string()


primitive SourceIdsQueryEncoder
  fun response(source_ids: Array[String] val): String =>
    let o = JsonObject
    let ids = JsonArray
    for id in source_ids.values() do ids.data.push(id) end
    o.data("source_ids") = ids
    o.string()


primitive ShrinkQueryJsonDecoder
  fun request(json: String): ExternalShrinkRequestMsg ? =>
    let doc = JsonDoc
    doc.parse(json)?
    let obj: JsonObject = doc.data as JsonObject
    let query = obj.data("query")? as Bool
    let arr: JsonArray = obj.data("node_names")? as JsonArray
    let node_count = U64.from[I64](obj.data("node_count")? as I64)
    let node_names: Array[String] trn = recover trn Array[String] end
    for n in arr.data.values() do node_names.push(n as String) end
    ExternalShrinkRequestMsg(query, consume node_names, node_count)

  fun response(json: String): ExternalShrinkQueryResponseMsg ? =>
    let doc = JsonDoc
    doc.parse(json)?
    let obj: JsonObject = doc.data as JsonObject
    let arr: JsonArray = obj.data("node_names")? as JsonArray
    let node_count = U64.from[I64](obj.data("node_count")? as I64)
    let node_names: Array[String] trn = recover trn Array[String] end
    for n in arr.data.values() do node_names.push(n as String) end
    ExternalShrinkQueryResponseMsg(consume node_names, node_count)


primitive ClusterStatusQueryJsonDecoder
  fun response(json: String): ExternalClusterStatusQueryResponseMsg ? =>
    let doc = JsonDoc
    doc.parse(json)?
    let obj: JsonObject = doc.data as JsonObject
    let arr: JsonArray = obj.data("worker_names")? as JsonArray
    let wn: Array[String] trn = recover trn Array[String] end

    for n in arr.data.values() do wn.push(n as String) end
    let wc = U64.from[I64](obj.data("worker_count")? as I64)
    let p = obj.data("processing_messages")? as Bool
    ExternalClusterStatusQueryResponseMsg(wc, consume wn, p, json)


primitive SourceIdsQueryJsonDecoder
  fun response(json: String): ExternalSourceIdsQueryResponseMsg ? =>
    let doc = JsonDoc
    doc.parse(json)?
    let obj: JsonObject = doc.data as JsonObject
    let arr: JsonArray = obj.data("source_ids")? as JsonArray
    let sis: Array[String] trn = recover trn Array[String] end
    for id in arr.data.values() do sis.push(id as String) end
    ExternalSourceIdsQueryResponseMsg(obj.string())
