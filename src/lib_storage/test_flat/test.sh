#!/bin/sh
if [ ! -d ../../../tezos-node-mainnet/context ]; then
    echo You need a data directory ../../../tezos-node-mainnet/context
    echo Prelare it by:
    echo '$ cd ../../..'
    echo '$ make'
    echo '$ wget https://mainnet.xtz-shots.io/rolling -O tezos-mainnet.rolling'
    echo '$ ./tezos-node snapshot import tezos-mainnet.rolling --data-dir tezos-node-mainnet'
    exit 1
fi

rm -rf ../../../tezos-node-mainnet-copy
cp -a ../../../tezos-node-mainnet ../../../tezos-node-mainnet-copy
dune exec ./test.exe -- ../../../tezos-node-mainnet-copy/context CoWYb26o6jetBijjioprzNn7s5ppNFrLsPCBWJzBjDWTZtwnoya5
