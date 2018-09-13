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
use "wallaroo/ent/checkpoint"
use "wallaroo_labs/mort"

class val ShippedState
  let state_name: StateName
  let key: Key
  let state_bytes: ByteSeq val

  new create(state_name': StateName, key': Key, state_bytes': ByteSeq val) =>
    state_name = state_name'
    key = key'
    state_bytes = state_bytes'

primitive StepStateMigrator
  fun receive_state(step: Step ref, runner: Runner, state_name: StateName,
    key: Key, state: ByteSeq val)
  =>
    ifdef "trace" then
      @printf[I32]("Received new state\n".cstring())
    end
    match runner
    | let r: SerializableStateRunner =>
      r.import_key_state(step, state_name, key, state)
    else
      Fail()
    end

  fun send_state(runner: Runner, id: RoutingId, boundary: OutgoingBoundary,
    state_name: String, key: Key, checkpoint_id: CheckpointId,
    auth: AmbientAuth)
  =>
    match runner
    | let r: SerializableStateRunner =>
      let shipped_state = ShippedState(state_name, key,
        r.export_key_state(key))
      let shipped_state_bytes =
        try
          Serialised(SerialiseAuth(auth), shipped_state)?
            .output(OutputSerialisedAuth(auth))
        else
          Fail()
          recover val Array[U8] end
        end
      boundary.migrate_step(id, state_name, key, checkpoint_id,
        shipped_state_bytes)
    else
      Fail()
    end
