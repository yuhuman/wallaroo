#!/bin/bash
set -eu
rc_branch_name=$1
for_version=$2

compile_for_sandy_bridge() {
  PREV_HEAD=$(git rev-parse --abbrev-ref HEAD)
  TARGET_CPU=sandybridge
  PONYC_OPTS='target_cpu=${TARGET_CPU}'
  git checkout "$rc_branch_name"
  make clean
  (make build-machida "$PONYC_OPTS")
  (cd utils/data_receiver && make "$PONYC_OPTS")
  (cd utils/cluster_shutdown && make "$PONYC_OPTS")
  (cd giles/sender && make "$PONYC_OPTS")

  zip -j9 .release/wallaroo_bin.zip \
      utils/cluster_shutdown/cluster_shutdown \
      utils/data_receiver/data_receiver \
      machida/build/machida \
      giles/sender/sender
  echo "------ Wallaroo binaries compiled for ${TARGET_CPU}"
  git checkout "$PREV_HEAD"
}

build_ami_with_packer() {
  (cd .release &&
    regions=$(echo -n "$(cat ami/regions)" | tr '\n' ',')
    for cmd in validate build; do
      packer ${cmd} \
         -var "ami_regions=${regions}" \
         -var "wallaroo_version=${for_version}" \
         ami/template.json
    done
  )
}

compile_for_sandy_bridge
build_ami_with_packer
