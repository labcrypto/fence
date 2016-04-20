#!/bin/bash

targets="node ack enq mstat pop stat"

targetBaseDir=/opt/naeem/kawthar/office

make

for target in $targets; do
  targetDir=$target
  if [ $target = "node" ]; then
    targetDir="."
  fi
  cmd="cp -rfv $targetDir/naeem-gate-slave-$target.out $targetBaseDir/dsg/$target"
  echo $cmd
  $cmd
  cmd="cp -rfv $targetDir/naeem-gate-slave-$target.out $targetBaseDir/gg/$target"
  echo $cmd
  $cmd
done

