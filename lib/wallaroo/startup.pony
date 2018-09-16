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
use "collections"
use "files"
use "itertools"
use "net"
use "signals"
use "time"
use "wallaroo_labs/hub"
use "wallaroo_labs/mort"
use "wallaroo_labs/options"
use "wallaroo/core/boundary"
use "wallaroo/core/common/"
use "wallaroo/core/initialization"
use "wallaroo/core/messages"
use "wallaroo/core/metrics"
use "wallaroo/core/topology"
use "wallaroo/ent/autoscale"
use "wallaroo/ent/barrier"
use "wallaroo/ent/data_receiver"
use "wallaroo/ent/cluster_manager"
use "wallaroo/ent/network"
use "wallaroo/ent/recovery"
use "wallaroo/ent/router_registry"
use "wallaroo/ent/checkpoint"
use "wallaroo/ent/spike"


actor Startup
  let _env: Env
  var _startup_options: StartupOptions = StartupOptions

  let _application: Application val
  let _app_name: String

  var _external_host: String = ""
  var _external_service: String = ""
  var _cluster_initializer: (ClusterInitializer | None) = None
  var _application_distributor: (ApplicationDistributor | None) = None
  var _swarm_manager_addr: String = ""
  var _event_log: (EventLog | None) = None
  var _joining_listener: (TCPListener | None) = None
  var _recovery_listener: (TCPListener | None) = None

  // RECOVERY FILES
  var _event_log_file: String = ""
  var _event_log_dir_filepath: (FilePath | None) = None
  var _event_log_file_basename: String = ""
  var _event_log_file_suffix: String = ""
  var _local_topology_file: String = ""
  var _local_keys_file: String = ""
  var _data_channel_file: String = ""
  var _control_channel_file: String = ""
  var _worker_names_file: String = ""
  var _connection_addresses_file: String = ""
  var _checkpoint_ids_file: String = ""

  var _connections: (Connections | None) = None

  let _disposables: SetIs[DisposableActor] = _disposables.create()
  var _is_joining: Bool = false
  var _is_recovering: Bool = false

  new create(env: Env, application: Application val,
    app_name: (String | None))
  =>
    _env = env
    _application = application
    _app_name = match app_name
      | let n: String => n
      else
        ""
      end
    ifdef "resilience" then
      @printf[I32]("****RESILIENCE MODE is active****\n".cstring())
      _print_link_to_community_license()
    end
    ifdef "clustering" then
      @printf[I32]("****CLUSTERING MODE is active****\n".cstring())
      _print_link_to_community_license()
    end
    ifdef "autoscale" then
      @printf[I32]("****AUTOSCALE MODE is active****\n".cstring())
      _print_link_to_community_license()
    end
    ifdef "trace" then
      @printf[I32]("****TRACE is active****\n".cstring())
    end
    ifdef "spike" then
      @printf[I32]("****SPIKE is active****\n".cstring())
    end

    try
      let auth = _env.root as AmbientAuth
      _startup_options = WallarooConfig.wallaroo_args(_env.args)?
      _set_recovery_file_names(auth)
      _is_recovering = is_recovering(auth)
      _is_joining = (not _is_recovering) and _startup_options.is_joining

      if _is_joining then
        @printf[I32]("New worker preparing to join cluster\n".cstring())
      elseif _is_recovering then
        @printf[I32]("Attempting to recover...\n".cstring())
      end

      (_external_host, _external_service) =
        match _startup_options.x_arg
        | let addr: Array[String] =>
          try
            (addr(0)?, addr(1)?)
          else
            Fail()
            ("", "")
          end
        else
          ("", "")
        end

      @printf[I32](("||| Resilience directory: " +
        _startup_options.resilience_dir + "|||\n").cstring())

      if not FilePath(auth, _startup_options.resilience_dir)?.exists() then
        @printf[I32](("Resilience directory: " +
          _startup_options.resilience_dir + " doesn't exist\n").cstring())
        error
      end

      ifdef "resilience" then
        @printf[I32](("||| Log-rotation: " +
          _startup_options.log_rotation.string() + "|||\n").cstring())
      end

      if _is_joining then
        if _startup_options.worker_name == "" then
          @printf[I32](("You must specify a name for the worker " +
            "via the -n parameter.\n").cstring())
          error
        end
        let j_addr = _startup_options.j_arg as Array[String]
        let control_notifier: TCPConnectionNotify iso =
          JoiningControlSenderConnectNotifier(auth,
            _startup_options.worker_name, _startup_options.worker_count, this)
        let control_conn: TCPConnection =
          TCPConnection(auth, consume control_notifier, j_addr(0)?, j_addr(1)?)
        _disposables.set(control_conn)
        // This only exists to keep joining worker alive while it waits for
        // cluster information.
        // TODO: Eliminate the need for this.
        let joining_listener = TCPListener(auth, JoiningListenNotifier)
        _joining_listener = joining_listener
        _disposables.set(joining_listener)
      elseif _is_recovering then
        (let checkpoint_id, let rollback_id) =
          LatestCheckpointId.read(auth, _checkpoint_ids_file)
        _initialize(checkpoint_id)
      else
        _initialize()
      end
    else
      StartupHelp()
    end

  be recover_and_initialize(checkpoint_id: CheckpointId) =>
    match _recovery_listener
    | let l: TCPListener =>
      l.dispose()
    end
    _initialize(checkpoint_id)

  fun ref _initialize(checkpoint_id: (CheckpointId | None) = None) =>
    try
      if _is_joining then
        Fail()
      end

      // TODO: Replace this with the name of this worker, whatever it
      // happens to be.
      let initializer_name = "initializer"

      let auth = _env.root as AmbientAuth

      let m_addr = _startup_options.m_arg as Array[String]

      if _startup_options.worker_name == "" then
        @printf[I32](("You must specify a worker name via " +
          "--name/-n.\n").cstring())
        error
      end

      // TODO::joining
      let connect_auth = TCPConnectAuth(auth)
      let metrics_conn = ReconnectingMetricsSink(m_addr(0)?,
          m_addr(1)?, _application.name(), _startup_options.worker_name)

      let event_log_dir_filepath = _event_log_dir_filepath as FilePath

      let event_log_filepath = try
        LastLogFilePath(_event_log_file_basename,
          _event_log_file_suffix, event_log_dir_filepath)?
      else
        FilePath(event_log_dir_filepath,
          _event_log_file_basename + _event_log_file_suffix)?
      end

      let local_topology_filepath: FilePath = FilePath(auth,
        _local_topology_file)?

      let local_keys_filepath: FilePath = FilePath(auth, _local_keys_file)?

      let data_channel_filepath: FilePath = FilePath(auth,
        _data_channel_file)?

      let control_channel_filepath: FilePath = FilePath(auth,
        _control_channel_file)?

      let worker_names_filepath: FilePath = FilePath(auth,
        _worker_names_file)?

      let connection_addresses_filepath: FilePath = FilePath(auth,
        _connection_addresses_file)?

      let checkpoint_ids_filepath: FilePath = FilePath(auth,
        _checkpoint_ids_file)?

      _event_log = ifdef "resilience" then
        if _startup_options.log_rotation then
          EventLog(auth, _startup_options.worker_name,
            EventLogConfig(event_log_dir_filepath,
            _event_log_file_basename
            where backend_file_length' =
              _startup_options.event_log_file_length,
            suffix' = _event_log_file_suffix, log_rotation' = true,
            is_recovering' = _is_recovering))
        else
          EventLog(auth, _startup_options.worker_name,
            EventLogConfig(event_log_dir_filepath,
            _event_log_file_basename + _event_log_file_suffix
            where backend_file_length' =
              _startup_options.event_log_file_length,
              is_recovering' = _is_recovering))
        end
      else
        EventLog(auth, _startup_options.worker_name,
          EventLogConfig(where is_recovering' = _is_recovering))
      end
      let event_log = _event_log as EventLog

      let connections = Connections(_application.name(),
        _startup_options.worker_name, auth,
        _startup_options.c_host, _startup_options.c_service,
        _startup_options.d_host, _startup_options.d_service,
        metrics_conn, m_addr(0)?, m_addr(1)?, _startup_options.is_initializer,
        _connection_addresses_file, _is_joining,
        _startup_options.spike_config, event_log,
        _startup_options.log_rotation where recovery_file_cleaner = this)
      _connections = connections
      connections.register_disposable(this)

      let barrier_initiator = BarrierInitiator(auth,
        _startup_options.worker_name, connections, initializer_name,
        _is_recovering)
      connections.register_disposable(barrier_initiator)
      event_log.set_barrier_initiator(barrier_initiator)

      // TODO: We currently set the primary checkpoint initiator worker to the
      // initializer.
      let checkpoint_initiator = CheckpointInitiator(auth,
        _startup_options.worker_name, initializer_name, connections,
        _startup_options.time_between_checkpoints, event_log,
        barrier_initiator, _checkpoint_ids_file,
        _startup_options.checkpoints_enabled
        where is_recovering = _is_recovering)
      checkpoint_initiator.initialize_checkpoint_id()
      connections.register_disposable(checkpoint_initiator)

      let autoscale_initiator = AutoscaleInitiator(
        _startup_options.worker_name, barrier_initiator)
      connections.register_disposable(autoscale_initiator)

      _setup_shutdown_handler(connections, this, auth)

      let data_receivers = DataReceivers(auth, connections,
        _startup_options.worker_name, _is_recovering)

      let router_registry = RouterRegistry(auth,
        _startup_options.worker_name, data_receivers,
        connections, this,
        _startup_options.stop_the_world_pause, _is_joining, initializer_name,
        barrier_initiator, checkpoint_initiator, autoscale_initiator,
        initializer_name)
      router_registry.set_event_log(event_log)

      (let control_host, let control_service, let data_service) =
        if _startup_options.is_initializer then
          (_startup_options.c_host, _startup_options.c_service,
            _startup_options.d_service)
        else
          (_startup_options.my_c_host, _startup_options.my_c_service,
            _startup_options.my_d_service)
        end

      let recovery_reconnecter = RecoveryReconnecter(auth,
        _startup_options.worker_name, data_service,
        data_receivers, router_registry,
        connections, _is_recovering)

      let recovery = Recovery(auth, _startup_options.worker_name,
        event_log, recovery_reconnecter, checkpoint_initiator, connections,
        router_registry, data_receivers)

      let local_topology_initializer =
        LocalTopologyInitializer(
          _application, _startup_options.worker_name,
          _env, auth, connections, router_registry, metrics_conn,
          _startup_options.is_initializer, data_receivers, event_log, recovery,
          recovery_reconnecter, checkpoint_initiator, barrier_initiator,
          _local_topology_file, _data_channel_file, _worker_names_file,
          local_keys_filepath)

      if (_external_host != "") or (_external_service != "") then
        let external_channel_notifier =
          ExternalChannelListenNotifier(_startup_options.worker_name, auth,
            connections, this, local_topology_initializer)
        let external_listener = TCPListener(auth,
          consume external_channel_notifier, _external_host, _external_service)
        connections.register_disposable(external_listener)
        @printf[I32]("Set up external channel listener on %s:%s\n".cstring(),
          _external_host.cstring(), _external_service.cstring())
      end

      if _startup_options.is_initializer then
        @printf[I32]("Running as Initializer...\n".cstring())
        _application_distributor = ApplicationDistributor(auth, _application,
          local_topology_initializer)

        match _application_distributor
        | let ad: ApplicationDistributor =>
          match _startup_options.worker_count
          | let wc: USize =>
            _cluster_initializer = ClusterInitializer(auth,
              _startup_options.worker_name, wc, connections, ad,
              local_topology_initializer, _startup_options.d_addr,
              metrics_conn, _is_recovering, initializer_name)
          else
            Unreachable()
          end
        end
      end

      let control_notifier: TCPListenNotify iso =
        ControlChannelListenNotifier(_startup_options.worker_name,
          auth, connections, _startup_options.is_initializer,
          _cluster_initializer, local_topology_initializer, recovery,
          recovery_reconnecter, router_registry, barrier_initiator,
          checkpoint_initiator, control_channel_filepath,
          _startup_options.my_d_host, _startup_options.my_d_service, event_log,
          this)

      // We need to recover connections before creating our control
      // channel listener, since it's at that point that we notify
      // the cluster of our control address. If the cluster connection
      // addresses were not yet recovered, we'd only notify the initializer.
      if _is_recovering then
        match checkpoint_id
        | let s_id: CheckpointId =>
          connections.recover_connections(local_topology_initializer, s_id)
        else
          Fail()
        end
      end

      if _startup_options.is_initializer then
        connections.make_and_register_recoverable_listener(
          auth, consume control_notifier, control_channel_filepath,
          _startup_options.c_host, _startup_options.c_service)
      else
        connections.make_and_register_recoverable_listener(
          auth, consume control_notifier, control_channel_filepath,
          _startup_options.my_c_host, _startup_options.my_c_service)
      end

      if _is_recovering then
        // need to do this before recreating the data connection as at
        // that point replay starts
        let recovered_workers = _recover_worker_names(
          worker_names_filepath)
        if recovered_workers.size() > 1 then
          local_topology_initializer.recover_and_initialize(
            recovered_workers, checkpoint_id as CheckpointId,
            _cluster_initializer)
        else
          local_topology_initializer.initialize(
            where checkpoint_target = checkpoint_id)
        end
      end

      if not _is_recovering then
        match _cluster_initializer
        | let ci: ClusterInitializer =>
          // TODO: Pass in initializer name once we've refactored
          // to expect it at .start() for pipeline apps.
          ci.start()
        end
      end
    else
      StartupHelp()
    end

  be complete_join(info_sending_host: String, m: InformJoiningWorkerMsg) =>
    try
      let auth = _env.root as AmbientAuth

      let local_keys_filepath: FilePath = FilePath(auth,
        _local_keys_file)?

      // TODO: Replace this with the name of this worker, whatever it
      // happens to be.
      let initializer_name = "initializer"

      let metrics_conn = ReconnectingMetricsSink(m.metrics_host,
        m.metrics_service, _application.name(), _startup_options.worker_name)

      // TODO: Are we creating connections to all addresses or just
      // initializer?
      (let c_host, let c_service) =
        if m.sender_name == "initializer" then
          (info_sending_host, m.control_addrs("initializer")?._2)
        else
          m.control_addrs("initializer")?
        end

      (let d_host, let d_service) =
        if m.sender_name == "initializer" then
          (info_sending_host, m.data_addrs("initializer")?._2)
        else
          m.data_addrs("initializer")?
        end

      // Generate state routing ids
      let new_state_routing_ids_iso = recover iso Map[StateName, RoutingId] end
      let routing_id_gen = RoutingIdGenerator
      for state_name in m.partition_router_blueprints.keys() do
        let next_id = routing_id_gen()
        new_state_routing_ids_iso(state_name) = next_id
      end
      let new_state_routing_ids = consume val new_state_routing_ids_iso

      let event_log_dir_filepath = _event_log_dir_filepath as FilePath
      _event_log = ifdef "resilience" then
        if _startup_options.log_rotation then
          EventLog(auth, _startup_options.worker_name,
            EventLogConfig(event_log_dir_filepath,
            _event_log_file_basename
            where backend_file_length' =
              _startup_options.event_log_file_length,
            suffix' = _event_log_file_suffix, log_rotation' = true))
        else
          EventLog(auth, _startup_options.worker_name,
            EventLogConfig(event_log_dir_filepath,
            _event_log_file_basename + _event_log_file_suffix
            where backend_file_length' =
              _startup_options.event_log_file_length))
        end
      else
        EventLog(auth, _startup_options.worker_name)
      end
      let event_log = _event_log as EventLog

      let connections = Connections(_application.name(),
        _startup_options.worker_name,
        auth, c_host, c_service, d_host, d_service,
        metrics_conn, m.metrics_host, m.metrics_service,
        _startup_options.is_initializer,
        _connection_addresses_file, _is_joining,
        _startup_options.spike_config, event_log,
        _startup_options.log_rotation where recovery_file_cleaner = this)
      _connections = connections
      connections.register_disposable(this)

      let barrier_initiator = BarrierInitiator(auth,
        _startup_options.worker_name, connections, initializer_name)
      event_log.set_barrier_initiator(barrier_initiator)

      let checkpoint_initiator = CheckpointInitiator(auth,
        _startup_options.worker_name, m.primary_checkpoint_worker, connections,
        _startup_options.time_between_checkpoints, event_log,
        barrier_initiator, _checkpoint_ids_file,
        _startup_options.checkpoints_enabled)
      checkpoint_initiator.initialize_checkpoint_id(
        (m.checkpoint_id, m.rollback_id))

      let autoscale_initiator = AutoscaleInitiator(
        _startup_options.worker_name, barrier_initiator)

      _setup_shutdown_handler(connections, this, auth)

      let data_receivers = DataReceivers(auth, connections,
        _startup_options.worker_name)

      let router_registry = RouterRegistry(auth,
        _startup_options.worker_name, data_receivers,
        connections, this,
        _startup_options.stop_the_world_pause, _is_joining, initializer_name,
        barrier_initiator, checkpoint_initiator, autoscale_initiator,
        m.sender_name where joining_state_routing_ids = new_state_routing_ids)
      router_registry.set_event_log(event_log)

      let recovery_reconnecter = RecoveryReconnecter(auth,
        _startup_options.worker_name, _startup_options.my_d_service,
        data_receivers, router_registry, connections)

      let recovery = Recovery(auth, _startup_options.worker_name,
        event_log, recovery_reconnecter, checkpoint_initiator, connections,
        router_registry, data_receivers)

      let local_topology_initializer =
        LocalTopologyInitializer(
          _application, _startup_options.worker_name,
          _env, auth, connections, router_registry, metrics_conn,
          _startup_options.is_initializer, data_receivers,
          event_log, recovery, recovery_reconnecter, checkpoint_initiator,
          barrier_initiator, _local_topology_file, _data_channel_file,
          _worker_names_file, local_keys_filepath
          where is_joining = true,
          joining_state_routing_ids = new_state_routing_ids)

      if (_external_host != "") or (_external_service != "") then
        let external_channel_notifier =
          ExternalChannelListenNotifier(_startup_options.worker_name, auth,
            connections, this, local_topology_initializer)
        let external_listener = TCPListener(auth,
          consume external_channel_notifier, _external_host, _external_service)
        connections.register_disposable(external_listener)
        @printf[I32]("Set up external channel listener on %s:%s\n".cstring(),
          _external_host.cstring(), _external_service.cstring())
      end

      let dr_state_routing_ids = recover iso Map[RoutingId, StateName] end
      for (s_name, r_id) in new_state_routing_ids.pairs() do
        dr_state_routing_ids(r_id) = s_name
      end

      let data_router = DataRouter(_startup_options.worker_name,
        recover Map[RoutingId, Consumer] end,
        recover Map[StateName, Array[Step] val] end,
        consume dr_state_routing_ids)

      router_registry.set_data_router(data_router)
      let updated_topology = m.local_topology.add_state_routing_ids(
        _startup_options.worker_name, new_state_routing_ids)
      local_topology_initializer.update_topology(updated_topology)
      local_topology_initializer.create_data_channel_listener(m.worker_names,
        _startup_options.my_d_host, _startup_options.my_d_service)

      // Prepare control and data addresses, but sub in correct host for
      // the worker that sent inform message (since it didn't know its
      // host string as seen externally)
      let control_addrs = recover trn Map[String, (String, String)] end
      let data_addrs = recover trn Map[String, (String, String)] end
      for (worker, addr) in m.control_addrs.pairs() do
        if m.sender_name == worker then
          control_addrs(worker) = (info_sending_host, addr._2)
        else
          control_addrs(worker) = addr
        end
      end
      for (worker, addr) in m.data_addrs.pairs() do
        if m.sender_name == worker then
          data_addrs(worker) = (info_sending_host, addr._2)
        else
          data_addrs(worker) = addr
        end
      end

      let control_channel_filepath: FilePath = FilePath(auth,
        _control_channel_file)?
      let control_notifier: TCPListenNotify iso =
        ControlChannelListenNotifier(_startup_options.worker_name,
          auth, connections, _startup_options.is_initializer,
          _cluster_initializer, local_topology_initializer, recovery,
          recovery_reconnecter, router_registry, barrier_initiator,
          checkpoint_initiator, control_channel_filepath,
          _startup_options.my_d_host, _startup_options.my_d_service,
          event_log, this)

      connections.make_and_register_recoverable_listener(
        auth, consume control_notifier, control_channel_filepath,
        _startup_options.my_c_host, _startup_options.my_c_service)

      // Call this on local cluster initializer instead of Connections
      // directly to make sure messages are processed in the create
      // initialization order
      local_topology_initializer.create_connections(consume control_addrs,
        consume data_addrs)
      local_topology_initializer.quick_initialize_data_connections()
      local_topology_initializer.set_partition_router_blueprints(
        m.partition_router_blueprints,
        m.stateless_partition_router_blueprints, m.target_id_router_blueprints)

      // Dispose of temporary listener
      match _joining_listener
      | let tcp_l: TCPListener =>
        tcp_l.dispose()
      else
        Fail()
      end
    else
      Fail()
    end

  fun ref _set_recovery_file_names(auth: AmbientAuth) =>
    try
      _event_log_dir_filepath = FilePath(auth, _startup_options.resilience_dir)?
    else
      Fail()
    end
    _event_log_file_basename = _app_name + "-" + _startup_options.worker_name
    _event_log_file_suffix = ".evlog"
    _event_log_file = _startup_options.resilience_dir + "/" + _app_name + "-" +
      _startup_options.worker_name + ".evlog"
    _local_topology_file = _startup_options.resilience_dir + "/" + _app_name +
      "-" + _startup_options.worker_name + ".local-topology"
    _local_keys_file = _startup_options.resilience_dir + "/" + _app_name +
      "-" + _startup_options.worker_name + ".local-keys"
    _data_channel_file = _startup_options.resilience_dir + "/" + _app_name +
      "-" + _startup_options.worker_name + ".tcp-data"
    _control_channel_file = _startup_options.resilience_dir + "/" + _app_name +
      "-" + _startup_options.worker_name + ".tcp-control"
    _worker_names_file = _startup_options.resilience_dir + "/" + _app_name +
      "-" + _startup_options.worker_name + ".workers"
    _connection_addresses_file = _startup_options.resilience_dir + "/" +
      _app_name + "-" + _startup_options.worker_name + ".connection-addresses"
    _checkpoint_ids_file = _startup_options.resilience_dir + "/" + _app_name +
      "-" + _startup_options.worker_name + ".checkpoint_ids"

  fun is_recovering(auth: AmbientAuth): Bool =>
    // check to see if we can recover
    // Use Set to make the logic explicit and clear
    let existing_files: Set[String] = Set[String]

    try
      let event_log_dir_filepath = _event_log_dir_filepath as FilePath

      let event_log_filepath = try
        let elf: FilePath = LastLogFilePath(_event_log_file_basename,
          _event_log_file_suffix, event_log_dir_filepath)?
        existing_files.set(elf.path)
        elf
      else
        let elf = FilePath(event_log_dir_filepath,
          _event_log_file_basename + _event_log_file_suffix)?
        if elf.exists() then
          existing_files.set(elf.path)
        end
        elf
      end

      let local_topology_filepath: FilePath = FilePath(auth,
        _local_topology_file)?
      if local_topology_filepath.exists() then
        existing_files.set(local_topology_filepath.path)
      end

      let local_keys_filepath: FilePath = FilePath(auth,
        _local_keys_file)?
      if local_keys_filepath.exists() then
        existing_files.set(local_keys_filepath.path)
      end

      let data_channel_filepath: FilePath = FilePath(auth,
        _data_channel_file)?
      if data_channel_filepath.exists() then
        existing_files.set(data_channel_filepath.path)
      end

      let control_channel_filepath: FilePath = FilePath(auth,
        _control_channel_file)?
      if control_channel_filepath.exists() then
        existing_files.set(control_channel_filepath.path)
      end

      let worker_names_filepath: FilePath = FilePath(auth,
        _worker_names_file)?
      if worker_names_filepath.exists() then
        existing_files.set(worker_names_filepath.path)
      end

      let connection_addresses_filepath: FilePath = FilePath(auth,
        _connection_addresses_file)?
      if connection_addresses_filepath.exists() then
        existing_files.set(connection_addresses_filepath.path)
      end

      let checkpoint_ids_filepath: FilePath = FilePath(auth,
        _checkpoint_ids_file)?
      if checkpoint_ids_filepath.exists() then
        existing_files.set(checkpoint_ids_filepath.path)
      end

      let required_files: Set[String] = Set[String]
      ifdef "resilience" then
        required_files.set(event_log_filepath.path)
      end
      required_files.set(local_topology_filepath.path)
      required_files.set(control_channel_filepath.path)
      required_files.set(worker_names_filepath.path)
      if not _startup_options.is_initializer then
        required_files.set(data_channel_filepath.path)
        required_files.set(connection_addresses_filepath.path)
      end

      // Only validate _all_ files exist if _any_ files exist.
      if existing_files.size() > 0 then
        // If any recovery file exists, but not all, then fail
        if (required_files.op_and(existing_files)) != required_files then
          @printf[I32](("Some resilience/recovery files are missing! "
            + "Cannot continue!\n").cstring())
            let files_missing = required_files.without(existing_files)
            let files_missing_str: String val = "\n    "
              .join(files_missing.values())
            @printf[I32]("The missing files are:\n    %s\n".cstring(),
              files_missing_str.cstring())
          Fail()
          false
        else
          @printf[I32]("Recovering from recovery files!\n".cstring())
          // we are recovering because all files exist
          true
        end
      else
        false
      end
    else
      Fail()
      false
    end

  be clean_recovery_files() =>
    @printf[I32]("Removing recovery files\n".cstring())
    _remove_file(_event_log_file)
    _remove_file(_local_topology_file)
    _remove_file(_local_keys_file)
    _remove_file(_data_channel_file)
    _remove_file(_control_channel_file)
    _remove_file(_worker_names_file)
    _remove_file(_connection_addresses_file)
    _remove_file(_checkpoint_ids_file)

    try
      let event_log_dir_filepath = _event_log_dir_filepath as FilePath
      let base_dir = Directory(event_log_dir_filepath)?

      let event_log_filenames = FilterLogFiles(_event_log_file_basename,
        _event_log_file_suffix, base_dir.entries()?)
      for fn in event_log_filenames.values() do
        _remove_file(event_log_dir_filepath.path + "/" + fn)
      end
    else
      Fail()
    end

    @printf[I32]("Recovery files removed.\n".cstring())

    match _connections
    | let c: Connections =>
      c.shutdown()
    else
      Fail()
    end

  be dispose() =>
    @printf[I32]("Shutting down Startup...\n".cstring())
    for d in _disposables.values() do
      d.dispose()
    end

  fun ref _remove_file(filename: String) =>
    @printf[I32]("...Removing %s...\n".cstring(), filename.cstring())
    @remove[I32](filename.cstring())

  fun ref _recover_worker_names(worker_names_filepath: FilePath):
    Array[String] val
  =>
    """
    Read in a list of the names of all workers after recovery.
    """
    let ws = recover trn Array[String] end

    let file = File(worker_names_filepath)
    for worker_name in file.lines() do
      ws.push(worker_name)
      @printf[I32]("recover_worker_names: %s\n".cstring(),
        worker_name.cstring())
    end

    ws

  fun ref _print_link_to_community_license() =>
    @printf[I32](("****This is an enterprise feature. You may need to " +
      "obtain a paid usage agreement to use this feature in production. See " +
      "the Wallaroo Community License at https://github.com/WallarooLabs/" +
      "wallaroo/blob/master/LICENSE.md for details, and also please visit " +
      "the page at http://www.wallaroolabs.com/pricing****\n").cstring())

  fun ref _setup_shutdown_handler(c: Connections, r: RecoveryFileCleaner,
    a: AmbientAuth)
  =>
    SignalHandler(WallarooShutdownHandler(c, r, a), Sig.int())
    SignalHandler(WallarooShutdownHandler(c, r, a), Sig.term())

class WallarooShutdownHandler is SignalNotify
  """
  Shutdown gracefully on SIGTERM and SIGINT
  """
  let _connections: Connections
  let _recovery_file_cleaner: RecoveryFileCleaner
  let _auth: AmbientAuth

  new iso create(c: Connections, r: RecoveryFileCleaner, a: AmbientAuth) =>
    _connections = c
    _recovery_file_cleaner = r
    _auth = a

  fun ref apply(count: U32): Bool =>
    _connections.clean_files_shutdown(_recovery_file_cleaner)
    false
