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

use "options"
use "wallaroo"
use "wallaroo/core/source"

class val PullSourceConfig[In: Any val]
  let _pull_source_integration: PullSourceIntegration[In] val

  new val create(psi: PullSourceIntegration[In] val) =>
    _pull_source_integration = psi

  fun source_listener_builder_builder(psli: PullSourceListenerIntegration):
    PullSourceListenerBuilderBuilder
  =>
    PullSourceListenerBuilderBuilder(psli)

  fun source_builder(app_name: String, pipeline_name: String):
    TypedPullSourceBuilderBuilder[In]
  =>
    TypedPullSourceBuilderBuilder[In](app_name, pipeline_name,
      _pull_source_integration)
