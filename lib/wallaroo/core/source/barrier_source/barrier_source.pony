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
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/core/routing"
use "wallaroo/core/source"
use "wallaroo/core/topology"
use "wallaroo/ent/barrier"
use "wallaroo/ent/router_registry"
use "wallaroo/ent/watermarking"
use "wallaroo_labs/mort"

actor BarrierSource is Source
  """
  This is an artificial source whose purpose is to ensure that we have
  at least one source available for injecting barriers into the system.
  It's possible (with TCPSources for example) for sources to come in and
  out of existence. We shouldn't depend on their presence to be able to
  inject barriers.
  """
  let _source_id: StepId
  // Routers for the pipelines with sources on this worker.
  var _routers: Map[String, Router] = _routers.create()
  // Map from identifier to pipeline names to interpret routers we receive.
  // Because multiple pipelines might route directly to the same state
  // collection, we map from the id to an array of pipeline names.
  var _pipeline_identifiers: Map[_PipelineIdentifier, SetIs[String]] =
    _pipeline_identifiers.create()

  let _router_registry: RouterRegistry

////////
  // Map from pipeline name to outputs for sources in that pipeline.
  let _pipeline_outputs: Map[String, Map[StepId, Consumer]] =
    _pipeline_outputs.create()

  // All outputs from this BarrierSource. There might be duplicate entries
  // across the _pipeline_outputs maps, so we use this for actually
  // sending barriers.
  let _outputs: Map[StepId, Consumer] = _outputs.create()
///////

  var _disposed: Bool = false

  ////////////////////
  // !@ Probably remove these
  // !@ Can we do without this? Probably, since we only send barriers.
  var _seq_id: SeqId = 1 // 0 is reserved for "not seen yet"
  let _acker_x: Acker = Acker
  ////////////////////

  new create(source_id: StepId, router_registry: RouterRegistry) =>
    """
    A new connection accepted on a server.
    """
    _source_id = source_id
    _router_registry = router_registry

  be register_pipeline(pipeline_name: String, router: Router) =>
    """
    On this worker, we need to keep track of every pipeline that has at least
    one Source. That's because we need to be able to forward barriers to
    everything a Source for that pipeline would forward to on this worker.
    """
    let p_identifier = _PipelineIdentifierCreator(router)
    try
      _pipeline_outputs.insert_if_absent(pipeline_name, Map[StepId, Consumer])?
      _pipeline_identifiers.insert_if_absent(p_identifier, SetIs[String])?
      _pipeline_identifiers(p_identifier)?.set(pipeline_name)
    else
      Fail()
    end

    // Subscribe to the router if it can be updated over time.
    match router
    | let pr: PartitionRouter =>
      _router_registry.register_partition_router_subscriber(pr.state_name(),
        this)
    | let spr: StatelessPartitionRouter =>
      _router_registry.register_stateless_partition_router_subscriber(
        spr.partition_id(), this)
    end
    _update_router(pipeline_name, router)

  be update_router(router: Router) =>
    let pid = _PipelineIdentifierCreator(router)
    try
      let pipelines = _pipeline_identifiers(pid)?
      for p in pipelines.values() do
        _update_router(p, router)
      end
    else
      Fail()
    end

  fun ref _update_router(pipeline_name: String, router: Router) =>
    if _routers.contains(pipeline_name) then
      try
        let old_router = _routers(pipeline_name)?
        _routers(pipeline_name) = router
        for (old_id, outdated_consumer) in
          old_router.routes_not_in(router).pairs()
        do
          _unregister_output(pipeline_name, old_id, outdated_consumer)
        end
      else
        Unreachable()
      end
    else
      _routers(pipeline_name) = router
    end
    for (c_id, consumer) in router.routes().pairs() do
      _register_output(pipeline_name, c_id, consumer)
    end

  be remove_route_to_consumer(id: StepId, c: Consumer) =>
    None

  fun ref _register_output(pipeline: String, id: StepId, c: Consumer) =>
    try
      if _pipeline_outputs(pipeline)?.contains(id) then
        try
          let old_c = _outputs(id)?
          if old_c is c then
            // We already know about this output.
            return
          end
          _unregister_output(pipeline, id, old_c)
        else
          Unreachable()
        end
      end

      _pipeline_outputs(pipeline)?(id) = c
      _outputs(id) = c
      match c
      | let ob: OutgoingBoundary =>
        ob.forward_register_producer(_source_id, id, this)
      else
        c.register_producer(_source_id, this)
      end
    else
      Fail()
    end

  fun ref _unregister_all_outputs() =>
    """
    This method should only be called if we are removing this source from the
    active graph (or on dispose())
    """
    for (pipeline, outputs) in _pipeline_outputs.pairs() do
      for (id, consumer) in outputs.pairs() do
        // @printf[I32]("!@ -- _unregister_all_outputs\n".cstring())
        _unregister_output(pipeline, id, consumer)
      end
    end

  fun ref _unregister_output(pipeline: String, id: StepId, c: Consumer) =>
    try
      _pipeline_outputs(pipeline)?.remove(id)?
      match c
      | let ob: OutgoingBoundary =>
        ob.forward_unregister_producer(_source_id, id, this)
      else
        c.unregister_producer(_source_id, this)
      end
      var last_one = true
      for (p, outputs) in _pipeline_outputs.pairs() do
        if outputs.contains(id) then last_one = false end
      end
      if last_one then
        _outputs.remove(id)?
      end
    else
      Fail()
    end

  be add_boundary_builders(
    boundary_builders: Map[String, OutgoingBoundaryBuilder] val)
  =>
    """
    BarrierSource should not have its own OutgoingBoundaries, but should
    instead use the canonical ones for this worker.
    """
    None

  be remove_boundary(worker: String) =>
    None

  be reconnect_boundary(target_worker_name: String) =>
    None

  be mute(c: Consumer) =>
    None

  be unmute(c: Consumer) =>
    None

  be initiate_barrier(token: BarrierToken) =>
    @printf[I32]("!@ BarrierSource initiate_barrier\n".cstring())
    for (o_id, o) in _outputs.pairs() do
      match o
      | let ob: OutgoingBoundary =>
        ob.forward_barrier(o_id, _source_id, token)
      else
        o.receive_barrier(_source_id, this, token)
      end
    end

  be barrier_complete(token: BarrierToken) =>
    @printf[I32]("!@ barrier_complete at BarrierSource %s\n".cstring(), _source_id.string().cstring())
    None

  be report_status(code: ReportStatusCode) =>
    None

  be dispose() =>
    if not _disposed then
      _unregister_all_outputs()
      _router_registry.unregister_source(this, _source_id)
      @printf[I32]("Shutting down BarrierSource\n".cstring())
      _disposed = true
    end

  ///////////////////////
  // STUFF TO BE REMOVED
  ///////////////////////
  fun ref route_to(c: Consumer): (Route | None) =>
    None

  fun ref next_sequence_id(): SeqId =>
    _seq_id = _seq_id + 1

  fun ref current_sequence_id(): SeqId =>
    _seq_id

  fun ref _acker(): Acker =>
    _acker_x

  fun ref flush(low_watermark: U64) =>
    None

  be request_ack() =>
    None
