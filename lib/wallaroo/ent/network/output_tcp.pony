/*

Copyright 2017 The Wallaroo Authors.

Licensed as a Wallaroo Enterprise file under the Wallaroo Community
License (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

     https://github.com/wallaroolabs/wallaroo/blob/master/LICENSE

*/

use "net"

class OutNotify is TCPConnectionNotify
  let _name: String

  new iso create(name: String) =>
    _name = name

  fun ref connected(sock: TCPConnection ref) =>
    @printf[None]("%s outgoing connected\n".cstring(),
      _name.cstring())

  fun ref throttled(sock: TCPConnection ref) =>
    @printf[None]("%s outgoing throttled\n".cstring(),
      _name.cstring())

  fun ref unthrottled(sock: TCPConnection ref) =>
    @printf[None]("%s outgoing no longer throttled\n".cstring(),
      _name.cstring())
