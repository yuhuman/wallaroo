

actor PullSourceListener is SourceListener
  let _source_listener: SourceListenerIntegration
  let _step_id_gen: StepIdGenerator = StepIdGenerator
  let _env: Env
  var _router: Router
  let _router_registry: RouterRegistry
  let _route_builder: RouteBuilder
  var _outgoing_boundary_builders: Map[String, OutgoingBoundaryBuilder] val
  let _layout_initializer: LayoutInitializer
  let _metrics_reporter: MetricsReporter
  //!@
  // var _source_builder: SourceBuilder
  let _event_log: EventLog
  let _auth: AmbientAuth
  let _target_router: Router

  new create(env: Env, source_listener: SourceListenerIntegration,
    //!@
    // source_builder: SourceBuilder,
    router: Router,
    router_registry: RouterRegistry, route_builder: RouteBuilder,
    outgoing_boundary_builders: Map[String, OutgoingBoundaryBuilder] val,
    event_log: EventLog, auth: AmbientAuth,
    layout_initializer: LayoutInitializer,
    metrics_reporter: MetricsReporter iso,
    target_router: Router = EmptyRouter,
    host: String = "", service: String = "0", limit: USize = 0,
    init_size: USize = 64, max_size: USize = 16384)
  =>
    """
    Listens for both IPv4 and IPv6 connections.
    """
    _env = env
    _source_listener = source_listener
    _router = router
    _router_registry = router_registry
    _route_builder = route_builder
    _outgoing_boundary_builders = outgoing_boundary_builders
    _layout_initializer = layout_initializer
    _metrics_reporter = consume metrics_reporter
    //!@
    // _source_builder = source_builder
    _event_log = event_log
    _auth = auth
    _target_router = target_router

    match router
    | let pr: PartitionRouter =>
      _router_registry.register_partition_router_subscriber(pr.state_name(),
        this)
    | let spr: StatelessPartitionRouter =>
      _router_registry.register_stateless_partition_router_subscriber(
        spr.partition_id(), this)
    end

    //!@
    // @printf[I32]((source_builder.pipeline_name() + " source attempting to listen on "
    //   + host + ":" + service + "\n").cstring())
    // _notify_listening()

  be register_source(source_integration: PullSourceIntegration) =>
    let source_id = _step_id_gen()
    let source = PullSource(_auth, source_id, source_integration,
      _pipeline_name, _runner_builder, _router, _router.routes(),
      _route_builder, _outgoing_boundary_builders, _layout_initializer,
      _event_log, _metrics_reporter.clone(), _router_registry)
    // TODO: We need to figure out how to unregister this when the
    // source is disposed
    _router_registry.register_source(source)
    match _router
    | let pr: PartitionRouter =>
      _router_registry.register_partition_router_subscriber(pr.state_name(),
        source)
    | let spr: StatelessPartitionRouter =>
      _router_registry.register_stateless_partition_router_subscriber(
        spr.partition_id(), source)
    end
    // TODO: Should we be keeping a tally of how many sources are open, as
    // in TCPSourceListener?

  ///////////////
  // ROUTING
  ///////////////
  be update_router(router: Router) =>
    _source_builder = _source_builder.update_router(router)
    _router = router

  be remove_route_for(moving_step: Consumer) =>
    None

  be add_boundary_builders(
    boundary_builders: Map[String, OutgoingBoundaryBuilder] val)
  =>
    let new_builders = recover trn Map[String, OutgoingBoundaryBuilder] end
    // TODO: A persistent map on the field would be much more efficient here
    for (target_worker_name, builder) in _outgoing_boundary_builders.pairs() do
      new_builders(target_worker_name) = builder
    end
    for (target_worker_name, builder) in boundary_builders.pairs() do
      if not new_builders.contains(target_worker_name) then
        new_builders(target_worker_name) = builder
      end
    end
    _outgoing_boundary_builders = consume new_builders

  be update_boundary_builders(
    boundary_builders: Map[String, OutgoingBoundaryBuilder] val)
  =>
    _outgoing_boundary_builders = boundary_builders

  be remove_boundary(worker: String) =>
    let new_boundary_builders =
      recover iso Map[String, OutgoingBoundaryBuilder] end
    for (w, b) in _outgoing_boundary_builders.pairs() do
      if w != worker then new_boundary_builders(w) = b end
    end

    _outgoing_boundary_builders = consume new_boundary_builders

  //////////////
  // DISPOSE
  //////////////
  be dispose() =>
    @printf[I32]("Shutting down PullSourceListener\n".cstring())
    _source_listener.dispose()

