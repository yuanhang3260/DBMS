#!/bin/bash

rm -rf output/out_*

for i in {1..1000}
do
  echo $i
  ./test/BplusTree_test.out INDEX 1 > "output/out_$i"
done