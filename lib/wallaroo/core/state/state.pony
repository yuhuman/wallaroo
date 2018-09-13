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

use "buffered"
use "serialise"
use "wallaroo_labs/mort"

trait ref State
  fun write_log_entry(out_writer: Writer, auth: AmbientAuth) =>
    try
      let serialized =
        Serialised(SerialiseAuth(auth), this)?.output(OutputSerialisedAuth(auth))
      out_writer.write(serialized)
    else
      Fail()
    end

  fun read_log_entry(in_reader: Reader, auth: AmbientAuth): State ? =>
    try
      @printf[I32]("!@ State read_log_entry: BEFORE data, with %s reader bytes\n".cstring(), in_reader.size().string().cstring())
      let data: Array[U8] iso = in_reader.block(in_reader.size())?
      @printf[I32]("!@ State read_log_entry: AFTER data, with %s data bytes\n".cstring(), data.size().string().cstring())
      match Serialised.input(InputSerialisedAuth(auth), consume data)(
        DeserialiseAuth(auth))?
      | let s: State => s
      else
        @printf[I32]("!@ Ouch serialise gives something weird\n".cstring())
        error
      end
    else
      @printf[I32]("!@ Ouch serialise errors\n".cstring())
      error
    end

class EmptyState is State
