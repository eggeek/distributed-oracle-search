#!/bin/bash

execs=(
  "make_cpd_auto" 
  "gen_distribute_conf" 
  "fifo_auto"
)

function install() {
  exedir="./pathfinding/warthog/build/$1/bin"
  srcdir="./pathfinding/warthog"
  cd "$srcdir" && make "$1" -j
  cd -

  mkdir -p "bin"

  for i in "${execs[@]}"; do
    echo "install $i"
    cp "$exedir/$i" "bin/"
  done
}

case "$1" in
  "dev") install "dev" ;;
  "fast") install "fast" ;;
  *) install "dev" ;;
esac
