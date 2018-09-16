/*

Copyright 2018 The Wallaroo Authors.

Licensed as a Wallaroo Enterprise file under the Wallaroo Community
License (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

     https://github.com/wallaroolabs/wallaroo/blob/master/LICENSE

*/

use "collections"

use "wallaroo/core/common"
use "wallaroo/core/invariant"
use "wallaroo/ent/barrier"
use "wallaroo_labs/collection_helpers"
use "wallaroo_labs/mort"


trait _CheckpointInitiatorPhase
  fun name(): String

  fun ref checkpoint_barrier_complete(token: BarrierToken) =>
    _invalid_call()
    Fail()

  fun ref event_log_checkpoint_complete(worker: WorkerName,
    checkpoint_id: CheckpointId)
  =>
    _invalid_call()
    Fail()

  fun ref event_log_id_written(worker: WorkerName,
    checkpoint_id: CheckpointId)
  =>
    _invalid_call()
    Fail()

  fun _invalid_call() =>
    @printf[I32]("Invalid call on checkpoint initiator phase %s\n".cstring(),
      name().cstring())

class _WaitingCheckpointInitiatorPhase is _CheckpointInitiatorPhase
  fun name(): String => "_WaitingCheckpointInitiatorPhase"

class _CheckpointingPhase is _CheckpointInitiatorPhase
  let _token: CheckpointBarrierToken
  let _c_initiator: CheckpointInitiator ref
  var _barrier_complete: Bool = false
  var _event_log_checkpoints_complete: Bool = false
  let _acked_workers: SetIs[WorkerName] = _acked_workers.create()

  new create(token: CheckpointBarrierToken,
    c_initiator: CheckpointInitiator ref)
  =>
    _token = token
    _c_initiator = c_initiator

  fun name(): String => "_CheckpointingPhase"

  fun ref checkpoint_barrier_complete(token: BarrierToken) =>
    ifdef debug then
      Invariant(token == _token)
    end
    _barrier_complete = true
    _check_completion()

  fun ref event_log_checkpoint_complete(worker: WorkerName,
    checkpoint_id: CheckpointId)
  =>
    @printf[I32]("!@ _CheckpointingPhase: event_log_checkpoints_complete from %s for %s\n".cstring(), worker.cstring(), checkpoint_id.string().cstring())
    ifdef debug then
      Invariant(checkpoint_id == _token.id)
      Invariant(_c_initiator.workers().contains(worker))
    end
    _acked_workers.set(worker)
    @printf[I32]("!@ _CheckpointingPhase: acked_workers: %s, workers: %s\n".cstring(), _acked_workers.size().string().cstring(), _c_initiator.workers().size().string().cstring())
    if (_acked_workers.size() == _c_initiator.workers().size()) then
      _event_log_checkpoints_complete = true
      _check_completion()
    end

  fun ref _check_completion() =>
    if _barrier_complete and _event_log_checkpoints_complete then
      _c_initiator.event_log_write_checkpoint_id(_token.id, _token)
    end

class _WaitingForEventLogIdWrittenPhase is _CheckpointInitiatorPhase
  let _token: CheckpointBarrierToken
  let _c_initiator: CheckpointInitiator ref
  let _acked_workers: SetIs[WorkerName] = _acked_workers.create()

  new create(token: CheckpointBarrierToken,
    c_initiator: CheckpointInitiator ref)
  =>
    _token = token
    _c_initiator = c_initiator

  fun name(): String => "_WaitingForEventLogIdWrittenPhase"

  fun ref event_log_id_written(worker: WorkerName,
    checkpoint_id: CheckpointId)
  =>
    @printf[I32]("!@ _WaitingForEventLogIdWrittenPhase: event_log_id_written from %s for %s\n".cstring(), worker.cstring(), checkpoint_id.string().cstring())
    ifdef debug then
      Invariant(checkpoint_id == _token.id)
      Invariant(_c_initiator.workers().contains(worker))
    end
    _acked_workers.set(worker)
    @printf[I32]("!@ _WaitingForEventLogIdWrittenPhase: acked_workers: %s, workers: %s\n".cstring(), _acked_workers.size().string().cstring(), _c_initiator.workers().size().string().cstring())
    if (_acked_workers.size() == _c_initiator.workers().size()) then
      _c_initiator.checkpoint_complete(_token)
    end
