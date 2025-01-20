#!/usr/bin/env bash
set -e

# Perform fossil collection and fossil deletion for all manifests from a Vinculum repository.

repository="$1"
shift
echo "Pruning manifests for Vinculum repository $repository ..."
for manifest in "$@"
do
	echo "Pruning manifest $manifest ..."
	(time vinculum-benchmark collect "$repository" /tmp/vinculum_benchmark.cbor "$manifest" --parallelism 1) 2>&1
	# perform empty backup to allow fossil deletion
	sleep 1
	vinculum-benchmark create "$repository" "empty_$manifest" benchmark /dev/null
	(time vinculum-benchmark delete "$repository" /tmp/vinculum_benchmark.cbor --parallelism 1) 2>&1
done
