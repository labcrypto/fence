#!/bin/bash

targets="node ack enq sack spop pop stat"

targetBaseDir=/opt/naeem/kawthar/center

make

for target in $targets; do
  targetDir=$target
  if [ $target = "node" ]; then
    targetDir="."
  fi
  cmd="cp -rfv $targetDir/naeem-gate-master-$target.out $targetBaseDir/dsgm/$target"
  echo $cmd
  $cmd
  cmd="cp -rfv $targetDir/naeem-gate-master-$target.out $targetBaseDir/ggm/$target"
  echo $cmd
  $cmd
done

