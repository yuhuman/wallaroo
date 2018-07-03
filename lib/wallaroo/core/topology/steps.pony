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

use "assert"
use "buffered"
use "collections"
use "net"
use "serialise"
use "time"
use "wallaroo_labs/guid"
use "wallaroo_labs/time"
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/core/state"
use "wallaroo/ent/data_receiver"
use "wallaroo/ent/network"
use "wallaroo/ent/rebalancing"
use "wallaroo/ent/recovery"
use "wallaroo/ent/snapshot"
use "wallaroo/ent/watermarking"
use "wallaroo_labs/mort"
use "wallaroo/core/initialization"
use "wallaroo/core/invariant"
use "wallaroo/core/metrics"
use "wallaroo/core/routing"
use "wallaroo/core/sink/tcp_sink"

actor Step is (Producer & Consumer)
  let _auth: AmbientAuth
  var _id: U128
  let _runner: Runner
  var _router: Router = EmptyRouter
  // For use if this is a state step, otherwise EmptyTargetIdRouter
  var _target_id_router: TargetIdRouter
  let _metrics_reporter: MetricsReporter
  // list of envelopes
  let _deduplication_list: DeduplicationList = _deduplication_list.create()
  let _event_log: EventLog
  var _seq_id_generator: StepSeqIdGenerator = StepSeqIdGenerator

  var _step_message_processor: StepMessageProcessor = EmptyStepMessageProcessor

  // _routes contains one route per Consumer
  let _routes: MapIs[Consumer, Route] = _routes.create()
  // _outputs keeps track of all output targets by step id. There might be
  // duplicate consumers in this map (unlike _routes) since there might be
  // multiple target step ids over a boundary
  let _outputs: Map[StepId, Consumer] = _outputs.create()
  // _routes contains one upstream per producer
  var _upstreams: SetIs[Producer] = _upstreams.create()
  // _inputs keeps track of all inputs by step id. There might be
  // duplicate producers in this map (unlike upstreams) since there might be
  // multiple upstream step ids over a boundary
  let _inputs: Map[StepId, Producer] = _inputs.create()

  // Lifecycle
  var _initializer: (LocalTopologyInitializer | None) = None
  var _initialized: Bool = false
  var _seq_id_initialized_on_recovery: Bool = false
  var _ready_to_work_routes: SetIs[RouteLogic] = _ready_to_work_routes.create()
  var _in_flight_ack_waiter: InFlightAckWaiter
  let _recovery_replayer: RecoveryReplayer

  let _acker_x: Acker = Acker

  let _outgoing_boundaries: Map[String, OutgoingBoundary] =
    _outgoing_boundaries.create()

  new create(auth: AmbientAuth, runner: Runner iso,
    metrics_reporter: MetricsReporter iso,
    id: U128, event_log: EventLog,
    recovery_replayer: RecoveryReplayer,
    outgoing_boundaries: Map[String, OutgoingBoundary] val,
    router: Router = EmptyRouter,
    target_id_router: TargetIdRouter = EmptyTargetIdRouter)
  =>
    _auth = auth
    _runner = consume runner
    match _runner
    | let r: ReplayableRunner => r.set_step_id(id)
    end
    _metrics_reporter = consume metrics_reporter
    _target_id_router = target_id_router
    _event_log = event_log
    _recovery_replayer = recovery_replayer
    _recovery_replayer.register_step(this)
    _id = id
    _in_flight_ack_waiter = InFlightAckWaiter(_id)

    for (worker, boundary) in outgoing_boundaries.pairs() do
      _outgoing_boundaries(worker) = boundary
    end
    _event_log.register_resilient(this, id)

    let initial_router = _runner.clone_router_and_set_input_type(router)
    _update_router(initial_router)

    for (c_id, consumer) in _router.routes().pairs() do
      _register_output(c_id, consumer)
    end

    for r in _routes.values() do
      ifdef "resilience" then
        _acker_x.add_route(r)
      end
    end

    _step_message_processor = NormalStepMessageProcessor(this)

  //
  // Application startup lifecycle event
  //
  be application_begin_reporting(initializer: LocalTopologyInitializer) =>
    initializer.report_created(this)

  be application_created(initializer: LocalTopologyInitializer) =>
    for r in _routes.values() do
      r.application_created()
      ifdef "resilience" then
        _acker_x.add_route(r)
      end
    end

    _initialized = true
    initializer.report_initialized(this)

  be application_initialized(initializer: LocalTopologyInitializer) =>
    _initializer = initializer
    if _routes.size() > 0 then
      for r in _routes.values() do
        r.application_initialized("Step")
      end
    else
      _report_ready_to_work()
    end

  fun ref report_route_ready_to_work(r: RouteLogic) =>
    if not _ready_to_work_routes.contains(r) then
      _ready_to_work_routes.set(r)

      if _ready_to_work_routes.size() == _routes.size() then
        _report_ready_to_work()
      end
    else
      // A route should only signal this once
      Fail()
    end

  fun ref _report_ready_to_work() =>
    match _initializer
    | let lti: LocalTopologyInitializer =>
      lti.report_ready_to_work(this)
    else
      Fail()
    end

  be application_ready_to_work(initializer: LocalTopologyInitializer) =>
    None

  be update_router(router: Router) =>
    _update_router(router)

  fun ref _update_router(router: Router) =>
    let old_router = _router
    _router = router
    for (old_id, outdated_consumer) in
      old_router.routes_not_in(_router).pairs()
    do
      if _outputs.contains(old_id) then
        _unregister_output(old_id, outdated_consumer)
      end
    end
    for (c_id, consumer) in _router.routes().pairs() do
      _register_output(c_id, consumer)
    end

  fun ref _register_output(id: StepId, c: Consumer) =>
    if _outputs.contains(id) then
      try
        let old_c = _outputs(id)?
        if old_c is c then
          // We already know about this output.
          return
        end
        _unregister_output(id, old_c)
      else
        Unreachable()
      end
    end

    _outputs(id) = c
    if not _routes.contains(c) then
      let new_route = RouteBuilder(_id, this, c, _metrics_reporter)
      _routes(c) = new_route
      ifdef "resilience" then
        _acker_x.add_route(new_route)
      end
      new_route.register_producer(id)
    else
      try
        _routes(c)?.register_producer(id)
      else
        Unreachable()
      end
    end

  fun ref _unregister_output(id: StepId, c: Consumer) =>
    try
      _routes(c)?.unregister_producer(id)
      _outputs.remove(id)?
      _remove_route_if_no_output(c)
    else
      Fail()
    end

  fun ref _unregister_all_outputs() =>
    """
    This method should only be called if we are removing this step from the
    active graph (or on dispose())
    """
    for (id, consumer) in _outputs.pairs() do
      _unregister_output(id, consumer)
    end

  be remove_route_to_consumer(id: StepId, c: Consumer) =>
    if _outputs.contains(id) then
      ifdef debug then
        Invariant(_routes.contains(c))
      end
      _unregister_output(id, c)
    end

  fun ref _remove_route_if_no_output(c: Consumer) =>
    var have_output = false
    for consumer in _outputs.values() do
      if consumer is c then have_output = true end
    end
    if not have_output then
      _remove_route(c)
    end

  fun ref _remove_route(c: Consumer) =>
    try
      let outdated_route = _routes.remove(c)?._2
      ifdef "resilience" then
        _acker_x.remove_route(outdated_route)
      end
    else
      Fail()
    end

  be update_target_id_router(target_id_router: TargetIdRouter) =>
    let old_router = _target_id_router
    _target_id_router = target_id_router
    for (old_id, outdated_consumer) in
      old_router.routes_not_in(_target_id_router).pairs()
    do
      if _outputs.contains(old_id) then
        _unregister_output(old_id, outdated_consumer)
      end
    end

    for (id, consumer) in target_id_router.routes().pairs() do
      _register_output(id, consumer)
    end

    _add_boundaries(target_id_router.boundaries())

  be add_boundaries(boundaries: Map[String, OutgoingBoundary] val) =>
    _add_boundaries(boundaries)

  fun ref _add_boundaries(boundaries: Map[String, OutgoingBoundary] val) =>
    for (worker, boundary) in boundaries.pairs() do
      if not _outgoing_boundaries.contains(worker) then
        _outgoing_boundaries(worker) = boundary
        let new_route = RouteBuilder(_id, this, boundary, _metrics_reporter)
        ifdef "resilience" then
          _acker_x.add_route(new_route)
        end
        _routes(boundary) = new_route
      end
    end

  be remove_boundary(worker: String) =>
    _remove_boundary(worker)

  fun ref _remove_boundary(worker: String) =>
    None
    //!@
    // try
    //   let old_ob = _outgoing_boundaries.remove(worker)?._2
    //   _routes(old_ob)?.dispose()
    //   for (id, c) in _outputs.pairs() do
    //     match c
    //     | let ob: OutgoingBoundary =>
    //       if ob is old_ob then
    //         _unregister_output(id, c)
    //       end
    //     end
    //   end
    // else
    //   Fail()
    // end

  be remove_route_for(step: Consumer) =>
    try
      _routes.remove(step)?
    else
      @printf[I32](("Tried to remove route for step but there was no route " +
        "to remove\n").cstring())
    end

  be run[D: Any val](metric_name: String, pipeline_time_spent: U64, data: D,
    i_producer_id: StepId, i_producer: Producer, msg_uid: MsgId,
    frac_ids: FractionalMessageId, i_seq_id: SeqId, i_route_id: RouteId,
    latest_ts: U64, metrics_id: U16, worker_ingress_ts: U64)
  =>
    ifdef "trace" then
      @printf[I32]("Received msg at Step\n".cstring())
    end
    _step_message_processor.run[D](metric_name, pipeline_time_spent, data,
      i_producer_id, i_producer, msg_uid, frac_ids, i_seq_id, i_route_id,
      latest_ts, metrics_id, worker_ingress_ts)

  fun ref process_message[D: Any val](metric_name: String,
    pipeline_time_spent: U64, data: D, i_producer_id: StepId,
    i_producer: Producer, msg_uid: MsgId, frac_ids: FractionalMessageId,
    i_seq_id: SeqId, i_route_id: RouteId, latest_ts: U64, metrics_id: U16,
    worker_ingress_ts: U64)
  =>
    _seq_id_generator.new_incoming_message()

    let my_latest_ts = ifdef "detailed-metrics" then
        Time.nanos()
      else
        latest_ts
      end

    let my_metrics_id = ifdef "detailed-metrics" then
        _metrics_reporter.step_metric(metric_name,
          "Before receive at step behavior", metrics_id, latest_ts,
          my_latest_ts)
        metrics_id + 1
      else
        metrics_id
      end

    ifdef "trace" then
      @printf[I32](("Rcvd msg at " + _runner.name() + " step\n").cstring())
    end

    (let is_finished, let last_ts) = _runner.run[D](metric_name,
      pipeline_time_spent, data, _id, this, _router, _target_id_router,
      msg_uid, frac_ids, my_latest_ts, my_metrics_id, worker_ingress_ts,
      _metrics_reporter)

    if is_finished then
      ifdef "resilience" then
        ifdef "trace" then
          @printf[I32]("Filtering\n".cstring())
        end
        _acker_x.filtered(this, current_sequence_id())
      end
      let end_ts = Time.nanos()
      let time_spent = end_ts - worker_ingress_ts

      ifdef "detailed-metrics" then
        _metrics_reporter.step_metric(metric_name, "Before end at Step", 9999,
          last_ts, end_ts)
      end

      _metrics_reporter.pipeline_metric(metric_name,
        time_spent + pipeline_time_spent)
      _metrics_reporter.worker_metric(metric_name, time_spent)
    end

    ifdef "resilience" then
      _acker_x.track_incoming_to_outgoing(current_sequence_id(), i_producer,
        i_route_id, i_seq_id)
    end

  fun ref next_sequence_id(): SeqId =>
    _seq_id_generator.new_id()

  fun ref current_sequence_id(): SeqId =>
    _seq_id_generator.latest_for_run()

  fun ref filter_queued_msg(i_producer: Producer, i_route_id: RouteId,
    i_seq_id: SeqId)
  =>
    """
    When we're in queuing mode, we currently immediately ack all messages that
    are queued. This can lead to lost messages if there is a worker failure
    immediately after a stop the world phase. This is a stopgap before we
    update the watermarking algorithm to migrate watermark information for
    migrated queued messages during step migration.
    TODO: Replace this strategy with a strategy for migrating watermark info
    for migrated queued messages during step migration.
    """
    _seq_id_generator.new_incoming_message()
    ifdef "resilience" then
      _acker_x.filtered(this, current_sequence_id())
      _acker_x.track_incoming_to_outgoing(current_sequence_id(), i_producer,
        i_route_id, i_seq_id)
    end

  ///////////
  // RECOVERY
  fun ref _is_duplicate(msg_uid: MsgId, frac_ids: FractionalMessageId): Bool =>
    MessageDeduplicator.is_duplicate(msg_uid, frac_ids, _deduplication_list)

  be replay_run[D: Any val](metric_name: String, pipeline_time_spent: U64,
    data: D, i_producer_id: StepId, i_producer: Producer, msg_uid: MsgId,
    frac_ids: FractionalMessageId, i_seq_id: SeqId, i_route_id: RouteId,
    latest_ts: U64, metrics_id: U16, worker_ingress_ts: U64)
  =>
    if not _is_duplicate(msg_uid, frac_ids) then
      _deduplication_list.push((msg_uid, frac_ids))

      process_message[D](metric_name, pipeline_time_spent, data, i_producer_id,
        i_producer, msg_uid, frac_ids, i_seq_id, i_route_id,
        latest_ts, metrics_id, worker_ingress_ts)
    else
      ifdef "trace" then
        @printf[I32]("Filtering a dupe in replay\n".cstring())
      end

      _seq_id_generator.new_incoming_message()

      ifdef "resilience" then
        _acker_x.filtered(this, current_sequence_id())
        _acker_x.track_incoming_to_outgoing(current_sequence_id(),
          i_producer, i_route_id, i_seq_id)
      end
    end

  //////////////////////
  // ORIGIN (resilience)
  be request_ack() =>
    _acker_x.request_ack(this)
    for route in _routes.values() do
      route.request_ack()
    end

  fun ref _acker(): Acker =>
    _acker_x

  fun ref flush(low_watermark: SeqId) =>
    ifdef "trace" then
      @printf[I32]("flushing at and below: %llu\n".cstring(), low_watermark)
    end
    _event_log.flush_buffer(_id, low_watermark)

  be log_replay_finished() => None

  be replay_log_entry(uid: U128, frac_ids: FractionalMessageId,
    statechange_id: U64, payload: ByteSeq val)
  =>
    if not _is_duplicate(uid, frac_ids) then
      ifdef "resilience" then
        StepLogEntryReplayer(_runner, _deduplication_list, uid, frac_ids,
          statechange_id, payload, this)
      end
    end

  be initialize_seq_id_on_recovery(seq_id: SeqId) =>
    ifdef debug then
      Invariant(_seq_id_initialized_on_recovery == false)
    end
    _seq_id_generator = StepSeqIdGenerator(seq_id)
    _seq_id_initialized_on_recovery = true

  be clear_deduplication_list() =>
    _deduplication_list.clear()

  be dispose() =>
    _unregister_all_outputs()

  fun ref route_to(c: Consumer): (Route | None) =>
    try
      _routes(c)?
    else
      None
    end

  fun has_route_to(c: Consumer): Bool =>
    _routes.contains(c)

  be register_producer(id: StepId, producer: Producer) =>
    @printf[I32]("!@ Registered producer %s at step %s. Total %s upstreams.\n".cstring(), id.string().cstring(), _id.string().cstring(), _upstreams.size().string().cstring())

    _inputs(id) = producer
    _upstreams.set(producer)

  be unregister_producer(id: StepId, producer: Producer) =>
    @printf[I32]("!@ Unregistered producer %s at step %s. Total %s upstreams.\n".cstring(), id.string().cstring(), _id.string().cstring(), _upstreams.size().string().cstring())

    // TODO: Investigate whether we need this Invariant or why it's sometimes
    // violated during shrink.
    // ifdef debug then
    //   Invariant(_upstreams.contains(producer))
    // end

    ifdef debug then
      Invariant(_inputs.contains(id))
    end

    if _inputs.contains(id) then
      try
        _inputs.remove(id)?
      else
        Fail()
      end

      var have_input = false
      for i in _inputs.values() do
        if i is producer then have_input = true end
      end
      if not have_input then
        _upstreams.unset(producer)
      end
    end

  be report_status(code: ReportStatusCode) =>
    match code
    | BoundaryCountStatus =>
      var b_count: USize = 0
      for r in _routes.values() do
        match r
        | let br: BoundaryRoute => b_count = b_count + 1
        end
      end
      @printf[I32]("Step %s has %s boundaries.\n".cstring(), _id.string().cstring(), b_count.string().cstring())
    end
    for r in _routes.values() do
      r.report_status(code)
    end

  be request_in_flight_ack(upstream_request_id: RequestId,
    requester_id: StepId, requester: InFlightAckRequester)
  =>
    match _step_message_processor
    | let nmp: NormalStepMessageProcessor =>
      _step_message_processor = QueueingStepMessageProcessor(this)
    end
    if not _in_flight_ack_waiter.already_added_request(requester_id) then
      _in_flight_ack_waiter.add_new_request(requester_id, upstream_request_id,
        requester)
      if _routes.size() > 0 then
        for r in _routes.values() do
          let request_id = _in_flight_ack_waiter.add_consumer_request(
            requester_id)
          r.request_in_flight_ack(request_id, _id, this)
        end
      else
        _in_flight_ack_waiter.try_finish_in_flight_request_early(requester_id)
      end
    else
      requester.receive_in_flight_ack(upstream_request_id)
    end

  be request_in_flight_resume_ack(in_flight_resume_ack_id: InFlightResumeAckId,
    request_id: RequestId, requester_id: StepId,
    requester: InFlightAckRequester, leaving_workers: Array[String] val)
  =>
    if _in_flight_ack_waiter.request_in_flight_resume_ack(in_flight_resume_ack_id,
      request_id, requester_id, requester)
    then
      for w in leaving_workers.values() do
        _remove_boundary(w)
      end
      match _step_message_processor
      | let qmp: QueueingStepMessageProcessor =>
        // Process all queued messages
        qmp.flush(_target_id_router)

        _step_message_processor = NormalStepMessageProcessor(this)
      end
      if _routes.size() > 0 then
        for r in _routes.values() do
          let new_request_id =
            _in_flight_ack_waiter.add_consumer_resume_request()
          r.request_in_flight_resume_ack(in_flight_resume_ack_id,
            new_request_id, _id, this, leaving_workers)
        end
      else
        _in_flight_ack_waiter.try_finish_resume_request_early()
      end
    end

  be try_finish_in_flight_request_early(requester_id: StepId) =>
    _in_flight_ack_waiter.try_finish_in_flight_request_early(requester_id)

  be receive_in_flight_ack(request_id: RequestId) =>
    _in_flight_ack_waiter.unmark_consumer_request(request_id)

  be receive_in_flight_resume_ack(request_id: RequestId) =>
    _in_flight_ack_waiter.unmark_consumer_resume_request(request_id)

  be mute(c: Consumer) =>
    for u in _upstreams.values() do
      u.mute(c)
    end

  be unmute(c: Consumer) =>
    for u in _upstreams.values() do
      u.unmute(c)
    end

  ///////////////
  // GROW-TO-FIT
  be receive_state(state: ByteSeq val) =>
    ifdef "autoscale" then
      try
        match Serialised.input(InputSerialisedAuth(_auth),
          state as Array[U8] val)(DeserialiseAuth(_auth))?
        | let shipped_state: ShippedState =>
          _step_message_processor = QueueingStepMessageProcessor(this,
            shipped_state.pending_messages)
          StepStateMigrator.receive_state(_runner, shipped_state.state_bytes)
        else
          Fail()
        end
      else
        Fail()
      end
    end

  be send_state_to_neighbour(neighbour: Step) =>
    ifdef "autoscale" then
      match _step_message_processor
      | let nmp: NormalStepMessageProcessor =>
        // TODO: Should this be possible?
        StepStateMigrator.send_state_to_neighbour(_runner, neighbour,
          Array[QueuedStepMessage], _auth)
      | let qmp: QueueingStepMessageProcessor =>
        StepStateMigrator.send_state_to_neighbour(_runner, neighbour,
          qmp.messages, _auth)
      end
    end
    _in_flight_ack_waiter.migrated()

  be send_state[K: (Hashable val & Equatable[K] val)](
    boundary: OutgoingBoundary, state_name: String, key: K)
  =>
    ifdef "autoscale" then
      //@!
      // _unregister_all_outputs()
      match _step_message_processor
      | let nmp: NormalStepMessageProcessor =>
        // TODO: Should this be possible?
        StepStateMigrator.send_state[K](_runner, _id, boundary, state_name,
          key, Array[QueuedStepMessage], _auth)
      | let qmp: QueueingStepMessageProcessor =>
        StepStateMigrator.send_state[K](_runner, _id, boundary, state_name,
          key, qmp.messages, _auth)
      end
    end
    _in_flight_ack_waiter.migrated()

  //////////////
  // SNAPSHOTS
  //////////////
  be receive_snapshot_barrier(step_id: StepId, sr: SnapshotRequester,
    snapshot_id: SnapshotId)
  =>
    if _step_message_processor.snapshot_in_progress() then
      _step_message_processor.receive_snapshot_barrier(step_id, sr,
        snapshot_id)
    else
      // !@ Handle this more cleanly. We need to make sure we don't lose
      // queued messages, but should snapshots ever be possible when we're in
      // the queueing state?
      match _step_message_processor
      | let nsmp: NormalStepMessageProcessor =>
        let sr_inputs = recover iso Map[StepId, SnapshotRequester] end
        for (sr_id, i) in _inputs.pairs() do
          sr_inputs(sr_id) = i
        end
        let s_outputs = recover iso Map[StepId, Snapshottable] end
        for (s_id, i) in _outputs.pairs() do
          s_outputs(s_id) = i
        end
        _step_message_processor = SnapshotStepMessageProcessor(this,
          SnapshotBarrierForwarder(_id, this, consume sr_inputs,
            consume s_outputs, snapshot_id))
      else
        Fail()
      end
    end

  be remote_snapshot_state() =>
    ifdef "resilience" then
      StepStateSnapshotter(_runner, _id, _seq_id_generator, _event_log)
    end

  fun ref snapshot_state(snapshot_id: SnapshotId) =>
    ifdef "resilience" then
      StepStateSnapshotter(_runner, _id, _seq_id_generator, _event_log)
    end

  fun ref snapshot_complete() =>
    ifdef debug then
      Invariant(_step_message_processor.snapshot_in_progress())
    end
    _step_message_processor = NormalStepMessageProcessor(this)
