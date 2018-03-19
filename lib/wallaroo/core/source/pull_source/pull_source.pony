

// trait tag PullSource is (Producer & Source)

actor PullSource is Producer
  let _source_id: StepId
  let _source: PullSourceIntegration
  let _pipeline_name: String
  let _msg_id_gen: MsgIdGenerator = MsgIdGenerator

  let _step_id_gen: StepIdGenerator = StepIdGenerator
  let _routes: MapIs[Consumer, Route] = _routes.create()
  let _route_builder: RouteBuilder
  let _outgoing_boundaries: Map[String, OutgoingBoundary] =
    _outgoing_boundaries.create()
  let _layout_initializer: LayoutInitializer
  var _unregistered: Bool = false

  let _metrics_reporter: MetricsReporter

  // Start muted. Wait for unmute to begin processing
  var _muted: Bool = true
  let _muted_downstream: SetIs[Any tag] = _muted_downstream.create()

  let _router_registry: RouterRegistry

  let _runner: Runner
  var _router: Router
  let _omni_router: OmniRouter = EmptyOmniRouter

  // Producer (Resilience)
  var _seq_id: SeqId = 1 // 0 is reserved for "not seen yet"

  new create(auth: AmbientAuth, s_id: StepId, s: SourceIntegration,
    pipeline_name: String, runner_builder: RunnerBuilder, router: Router,
    routes: Array[Consumer] val, route_builder: RouteBuilder,
    outgoing_boundary_builders: Map[String, OutgoingBoundaryBuilder] val,
    layout_initializer: LayoutInitializer, event_log: EventLog,
    metrics_reporter: MetricsReporter iso, router_registry: RouterRegistry,
    target_router: Router,
    pre_state_target_ids: Array[StepId] val = recover Array[StepId] end)
  =>
    _source_id = s_id
    _source = s
    _pipeline_name = pipeline_name

    _runner = runner_builder(event_log, auth, None,
      target_router, pre_state_target_ids)
    _router = _runner.clone_router_and_set_input_type(router)
    _routes = routes
    _route_builder = route_builder
    _layout_initializer = layout_initializer
    _metrics_reporter = consume metrics_reporter
    _router_registry = router_registry

    for (target_worker_name, builder) in outgoing_boundary_builders.pairs() do
      if not _outgoing_boundaries.contains(target_worker_name) then
        let new_boundary =
          builder.build_and_initialize(_step_id_gen(), _layout_initializer)
        router_registry.register_disposable(new_boundary)
        _outgoing_boundaries(target_worker_name) = new_boundary
      end
    end

    for consumer in routes.values() do
      _routes(consumer) =
        _route_builder(this, consumer, _metrics_reporter)
    end

    for (worker, boundary) in _outgoing_boundaries.pairs() do
      _routes(boundary) =
        _route_builder(this, boundary, _metrics_reporter)
    end

    // _notify.update_boundaries(_outgoing_boundaries)

    for r in _routes.values() do
      // TODO: this is a hack, we shouldn't be calling application events
      // directly. route lifecycle needs to be broken out better from
      // application lifecycle
      r.application_created()
    end

    for r in _routes.values() do
      r.application_initialized(_source.name())
    end

    _mute()

  //!@ TODO: This is the entry point called by user-defined source code
  be process[D: Any val](data: D) =>
    _metrics_reporter.pipeline_ingest(_pipeline_name, _source_name)
    let ingest_ts = Time.nanos()
    let pipeline_time_spent: U64 = 0
    var latest_metrics_id: U16 = 1

    ifdef "trace" then
      @printf[I32](("Rcvd msg at " + _pipeline_name + " source\n").cstring())
    end

    (let is_finished, let last_ts) =
      try
        let decode_end_ts = Time.nanos()
        _metrics_reporter.step_metric(_pipeline_name,
          "Decode Time in Source", latest_metrics_id, ingest_ts,
          decode_end_ts)
        latest_metrics_id = latest_metrics_id + 1

        ifdef "trace" then
          @printf[I32](("Msg decoded at " + _pipeline_name +
            " source\n").cstring())
        end
        _runner.run[D](_pipeline_name, pipeline_time_spent, data,
          conn, _router, _omni_router, _msg_id_gen(), None,
          decode_end_ts, latest_metrics_id, ingest_ts, _metrics_reporter)
      else
        @printf[I32](("Unable to decode message at " + _pipeline_name +
          " source\n").cstring())
        ifdef debug then
          Fail()
        end
        (true, ingest_ts)
      end

    if is_finished then
      let end_ts = Time.nanos()
      let time_spent = end_ts - ingest_ts

      ifdef "detailed-metrics" then
        _metrics_reporter.step_metric(_pipeline_name,
          "Before end at Source", 9999,
          last_ts, end_ts)
      end

      _metrics_reporter.pipeline_metric(_pipeline_name, time_spent +
        pipeline_time_spent)
      _metrics_reporter.worker_metric(_pipeline_name, time_spent)
    end

  //!@ TODO: This is how we request the next pull
  be next() =>
    if not _muted then
      _source.next()
      next()
    end

  ////////////////
  // ROUTING
  ////////////////
  fun ref route_to(c: Consumer): (Route | None) =>
    try
      _routes(c)?
    else
      None
    end

  fun ref next_sequence_id(): SeqId =>
    _seq_id = _seq_id + 1

  fun ref current_sequence_id(): SeqId =>
    _seq_id

  be update_router(router: PartitionRouter) =>
    let new_router =
      match router
      | let pr: PartitionRouter =>
        pr.update_boundaries(_outgoing_boundaries)
      | let spr: StatelessPartitionRouter =>
        spr.update_boundaries(_outgoing_boundaries)
      else
        router
      end

    for target in new_router.routes().values() do
      if not _routes.contains(target) then
        _routes(target) = _route_builder(this, target, _metrics_reporter)
      end
    end
    //!@
    // _notify.update_router(new_router)

  be remove_boundary(worker: String) =>
    if _outgoing_boundaries.contains(worker) then
      try
        let boundary = _outgoing_boundaries(worker)?
        _routes(boundary)?.dispose()
        _routes.remove(boundary)?
        _outgoing_boundaries.remove(worker)?
      else
        Fail()
      end
    end
    //!@
    // _notify.update_boundaries(_outgoing_boundaries)

  be add_boundary_builders(
    boundary_builders: Map[String, OutgoingBoundaryBuilder] val)
  =>
    """
    Build a new boundary for each builder that corresponds to a worker we
    don't yet have a boundary to. Each PullSource has its own
    OutgoingBoundary to each worker to allow for higher throughput.
    """
    for (target_worker_name, builder) in boundary_builders.pairs() do
      if not _outgoing_boundaries.contains(target_worker_name) then
        let boundary = builder.build_and_initialize(_step_id_gen(),
          _layout_initializer)
        _router_registry.register_disposable(boundary)
        _outgoing_boundaries(target_worker_name) = boundary
        _routes(boundary) =
          _route_builder(this, boundary, _metrics_reporter)
      end
    end
    //!@
    // _notify.update_boundaries(_outgoing_boundaries)

  ///////////////
  // Connections
  ///////////////
  be reconnect_boundary(target_worker_name: String) =>
    try
      _outgoing_boundaries(target_worker_name)?.reconnect()
    else
      Fail()
    end

  be mute(c: Consumer) =>
    _muted_downstream.set(c)
    _mute()

  be unmute(c: Consumer) =>
    _muted_downstream.unset(c)

    if _muted_downstream.size() == 0 then
      _unmute()
    end

  fun ref _mute() =>
    ifdef debug then
      @printf[I32]("Muting %s\n".cstring(), _source.name().cstring())
    end
    _muted = true
    _source.mute()

  fun ref _unmute() =>
    ifdef debug then
      @printf[I32]("Unmuting %s\n".cstring(), _source.name().cstring())
    end
    _muted = false
    if not _reading then
      _pending_reads()
    end
    _source.unmute()
    next()

  be dispose() =>
    @printf[I32]("Shutting down %s\n".cstring(), _source.name().cstring())
    for b in _outgoing_boundaries.values() do
      b.dispose()
    end
    _source.dispose()

  //////////////
  // ORIGIN (resilience)
  //    This source is not currently resilient
  //////////////
  be request_ack() =>
    None

  fun ref _acker(): Acker =>
    // TODO: we don't really need this
    // Because we dont actually do any resilience work
    Acker

  fun ref flush(low_watermark: U64) =>
    None

  be log_flushed(low_watermark: SeqId) =>
    None

  fun ref bookkeeping(o_route_id: RouteId, o_seq_id: SeqId) =>
    None

  be update_watermark(route_id: RouteId, seq_id: SeqId) =>
    ifdef "trace" then
      @printf[I32]("%s received update_watermark\n".cstring(),
        _source.name().cstring())
    end

  fun ref _update_watermark(route_id: RouteId, seq_id: SeqId) =>
    None
