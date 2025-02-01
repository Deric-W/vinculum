#!/usr/bin/env sh
set -e

# Prune all manifests from a Borg 2 repository and perform compaction.

repository="$1"
shift
echo "Pruning manifests for $(borg --version) repository $repository ..."
for manifest in "$@"
do
	echo "Pruning manifest $manifest ..."
	(time borg delete --repo "$repository" "$manifest") 2>&1
	(time borg compact --repo "$repository") 2>&1
done
