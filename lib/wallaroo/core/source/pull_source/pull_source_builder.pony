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

use "wallaroo/core/common"
use "wallaroo/core/metrics"
use "wallaroo/core/source"
use "wallaroo/core/topology"

class val TypedPullSourceBuilder[In: Any val] is SourceBuilder
  let _app_name: String
  let _worker_name: String
  let _pipeline_name: String
  let _runner_builder: RunnerBuilder
  //!@
  // let _handler: SH
  let _router: Router
  let _metrics_conn: MetricsSink
  let _pre_state_target_ids: Array[StepId] val
  let _metrics_reporter: MetricsReporter
  //!@
  // let _source_notify_builder: SourceNotifyBuilder[In, SH]

  new val create(app_name: String, worker_name: String,
    pipeline_name: String, runner_builder: RunnerBuilder,
    //!@
    // handler: SH,
    router: Router, metrics_conn: MetricsSink,
    pre_state_target_ids: Array[StepId] val = recover Array[StepId] end,
    metrics_reporter: MetricsReporter iso,
    //!@
    // source_notify_builder: SourceNotifyBuilder[In, SH]
    )
  =>
    _app_name = app_name
    _worker_name = worker_name
    _pipeline_name = pipeline_name
    _runner_builder = runner_builder
    //!@
    // _handler = handler
    _router = router
    _metrics_conn = metrics_conn
    _pre_state_target_ids = pre_state_target_ids
    _metrics_reporter = consume metrics_reporter
    //!@
    // _source_notify_builder = source_notify_builder

  fun pipeline_name(): String => _name

  fun apply(event_log: EventLog, auth: AmbientAuth, target_router: Router,
    env: Env): SourceNotify iso^
  =>
    _source_notify_builder(_name, env, auth, _handler, _runner_builder,
      _router, _metrics_reporter.clone(), event_log, target_router,
      _pre_state_target_ids)

  fun val update_router(router: Router): SourceBuilder =>
    TypedPullSourceBuilder[In](_app_name, _worker_name, _pipeline_name,
      _runner_builder, _handler, router, _metrics_conn, _pre_state_target_ids,
      _metrics_reporter.clone(),
      //!@
      //_source_notify_builder
      )

class val TypedPullSourceBuilderBuilder[In: Any val]
  let _app_name: String
  let _pipeline_name: String
  //!@
  // let _handler: FramedSourceHandler[In] val

  new val create(app_name: String, pipeline_name: String,
    //!@
    // handler: FramedSourceHandler[In] val)
    )
  =>
    _app_name = app_name
    _pipeline_name = pipeline_name
    //!@
    // _handler = handler

  fun name(): String => _name

  fun apply(runner_builder: RunnerBuilder, router: Router,
    metrics_conn: MetricsSink,
    pre_state_target_ids: Array[StepId] val = recover Array[StepId] end,
    worker_name: String, metrics_reporter: MetricsReporter iso):
      SourceBuilder
  =>
    TypedPullSourceBuilder[In](_app_name, worker_name,
      _pipeline_name, runner_builder, _handler, router,
      metrics_conn, pre_state_target_ids, consume metrics_reporter,
      //!@
      // TCPFramedSourceNotifyBuilder[In])
      )

