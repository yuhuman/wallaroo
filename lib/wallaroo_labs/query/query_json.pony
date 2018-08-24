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
use "../mort"
use "../../wallaroo/core/common"

type JsonDocUnion is (F64 val | I64 val | Bool val | None val | String val | JsonArray ref | JsonObject ref)

type _JsonDelimiters is (_JsonString | _JsonArray | _JsonMap)

primitive _JsonString
  fun apply(): (String, String) => ("\"", "\"")
primitive _JsonArray
  fun apply(): (String, String) => ("[", "]")
primitive _JsonMap
  fun apply(): (String, String) => ("{", "}")


primitive _Quoted
  fun apply(s: String): String => "\"" + s + "\""

type _StepIds is Array[String] val

type _StepIdsByWorker is Map[String, _StepIds] val

type _PartitionQueryMap is Map[String, _StepIdsByWorker] val

primitive _JsonEncoder
  fun apply(entries: Array[String] val,
    json_delimiters: _JsonDelimiters,
    quote_entries: Bool = false):
    String
  =>
    recover
      var s: Array[U8] iso = recover Array[U8] end
      s.>append(json_delimiters()._1)
       .>append(",".join(
           if quote_entries then
             Iter[String](entries.values()).map[String](
               {(s)=> _Quoted(s)})
           else
             entries.values()
           end
         ))
       .>append(json_delimiters()._2)
      String.from_array(consume s)
    end

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
    let entries = recover iso Array[String] end

    for (k, v) in digest.pairs() do
      entries.push(_Quoted(k) + ":" + _JsonEncoder(v, _JsonArray, true))
    end

    _JsonEncoder(consume entries, _JsonMap)

primitive StateEntityCountQueryEncoder
  fun state_entity_count(
    digest: Map[String, Array[String] val] val):
    String
  =>
    let entries = recover iso Array[String] end

    for (k, v) in digest.pairs() do
      entries.push(_Quoted(k) + ":" + v.size().string())
    end

    _JsonEncoder(consume entries, _JsonMap)

primitive StatelessPartitionQueryEncoder
  fun stateless_partition_keys(
    stateless_partitions: Map[String, Map[String, Array[String] val] val] val):
    String
  =>
    let entries = recover iso Array[String] end

    for (k, v) in stateless_partitions.pairs() do
      entries.push(_Quoted(k) + ":" + partitions_by_worker(v))
    end

    _JsonEncoder(consume entries, _JsonMap)

  fun partitions_by_worker(pbw: Map[String, Array[String] val] val): String =>
    let digest = recover iso Array[String] end
    for (k, v) in pbw.pairs() do
      digest.push(_Quoted(k) + ":" + _JsonEncoder(v, _JsonArray))
    end

    _JsonEncoder(consume digest, _JsonMap)

primitive StatelessPartitionCountQueryEncoder
  fun stateless_partition_count(
    stateless_partitions: Map[String, Map[String, Array[String] val] val] val):
    String
  =>
    let entries = recover iso Array[String] end

    for (k, v) in stateless_partitions.pairs() do
      entries.push(_Quoted(k) + ":" + partition_count_by_worker(v))
    end

    _JsonEncoder(consume entries, _JsonMap)

  fun partition_count_by_worker(pbw: Map[String, Array[String] val] val):
    String
  =>
    let digest = recover iso Array[String] end
    for (k, v) in pbw.pairs() do
      digest.push(_Quoted(k) + ":" + v.size().string())
    end

    _JsonEncoder(consume digest, _JsonMap)

primitive ShrinkQueryJsonEncoder
  fun request(query: Bool, node_names: Array[String] val, node_count: U64):
    String
  =>
    let entries = recover iso Array[String] end
    entries.push(_Quoted("query") + ":" + query.string())
    entries.push(_Quoted("node_names") + ":" + _JsonEncoder(node_names,
      _JsonArray))
    entries.push(_Quoted("node_count") + ":" + node_count.string())
    _JsonEncoder(consume entries, _JsonMap)

  fun response(node_names: Array[String] val, node_count: U64): String =>
    let entries = recover iso Array[String] end
    entries.push(_Quoted("node_names") + ":" + _JsonEncoder(node_names,
      _JsonArray))
    entries.push(_Quoted("node_count") + ":" + node_count.string())
    _JsonEncoder(consume entries, _JsonMap)

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


primitive JsonDecoder
  fun string_array(s: String): Array[String] val =>
    let items = recover iso Array[String] end
    var str = recover iso Array[U8] end
    if s.size() > 1 then
      for i in Range(1, s.size()) do
        let next_char = try s(i)? else Fail(); ' ' end
        if next_char == ',' then
          items.push(String.from_array(
            str = recover iso Array[U8] end))
        elseif (next_char != ']') and (next_char != '"') then
          str.push(next_char)
        end
      end
      items.push(String.from_array(str = recover iso Array[U8] end))
    end
    consume items

primitive ShrinkQueryJsonDecoder
  fun request(json: String): ExternalShrinkRequestMsg ? =>
    let p_map = Map[String, String]
    var is_key = true
    var this_key = ""
    var next_key = recover iso Array[U8] end
    var next_str = recover iso Array[U8] end
    for i in Range(1, json.size()) do
      let next_char = json(i)?
      if is_key then
        if next_char == ':' then
          is_key = false
          this_key = String.from_array(next_key = recover iso Array[U8] end)
        elseif (next_char != '"') and (next_char != ',') then
          next_key.push(next_char)
        end
      else
        let delimiter: U8 =
          match this_key
          | "query" => ','
          | "node_names" => ']'
          | "node_count" => '}'
          else error
          end
        if next_char == delimiter then
          let str = String.from_array(next_str = recover iso Array[U8] end)
          p_map(this_key) = str
          is_key = true
        else
          next_str.push(next_char)
        end
      end
    end

    let query_string = p_map("query")?
    let query = if query_string == "true" then true else false end

    let names_string = p_map("node_names")?
    let node_names = JsonDecoder.string_array(names_string)

    let node_count: U64 = p_map("node_count")?.u64()?

    ExternalShrinkRequestMsg(query, node_names, node_count)

  fun response(json: String): ExternalShrinkQueryResponseMsg ? =>
    let p_map = Map[String, String]
    var is_key = true
    var this_key = ""
    var next_key = recover iso Array[U8] end
    var next_str = recover iso Array[U8] end
    for i in Range(1, json.size()) do
      let next_char = json(i)?
      if is_key then
        if next_char == ':' then
          is_key = false
          this_key = String.from_array(next_key = recover iso Array[U8] end)
        elseif (next_char != '"') and (next_char != ',') then
          next_key.push(next_char)
        end
      else
        let delimiter: U8 =
          match this_key
          | "node_names" => ']'
          | "node_count" => '}'
          else error
          end
        if next_char == delimiter then
          let str = String.from_array(next_str = recover iso Array[U8] end)
          p_map(this_key) = str
          is_key = true
        else
          next_str.push(next_char)
        end
      end
    end

    let names_string = p_map("node_names")?
    let node_names = JsonDecoder.string_array(names_string)

    let node_count: U64 = p_map("node_count")?.u64()?

    ExternalShrinkQueryResponseMsg(node_names, node_count)

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
