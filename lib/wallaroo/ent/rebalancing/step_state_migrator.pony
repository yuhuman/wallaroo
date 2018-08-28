/*

Copyright 2017 The Wallaroo Authors.

Licensed as a Wallaroo Enterprise file under the Wallaroo Community
License (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

     https://github.com/wallaroolabs/wallaroo/blob/master/LICENSE

*/

use "collections"
use "serialise"
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/core/topology"
use "wallaroo/ent/snapshot"
use "wallaroo_labs/mort"

class val ShippedState
  let state_bytes: ByteSeq val

  new create(state_bytes': ByteSeq val)
  =>
    state_bytes = state_bytes'

primitive StepStateMigrator
  fun receive_state(runner: Runner, state: ByteSeq val)
  =>
    ifdef "trace" then
      @printf[I32]("Received new state\n".cstring())
    end
    match runner
    | let r: SerializableStateRunner =>
      r.replace_serialized_state(state)
    else
      Fail()
    end

  fun send_state(runner: Runner, id: RoutingId, boundary: OutgoingBoundary,
    state_name: String, key: Key, snapshot_id: SnapshotId, auth: AmbientAuth)
  =>
    match runner
    | let r: SerializableStateRunner =>
      let shipped_state = ShippedState(r.serialize_state())
      let shipped_state_bytes =
        try
          Serialised(SerialiseAuth(auth), shipped_state)?
            .output(OutputSerialisedAuth(auth))
        else
          Fail()
          recover val Array[U8] end
        end
      boundary.migrate_step(id, state_name, key, snapshot_id,
        shipped_state_bytes)
    else
      Fail()
    end
