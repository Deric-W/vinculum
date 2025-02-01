#!/usr/bin/env bash
set -e

# Perform fossil collection and fossil deletion for all revisions of a Duplicacy repository.

repository="$1"
shift
echo "Pruning revisions for Duplicacy for repository $repository ..."
touch "$repository/empty"
for revision in "$@"
do
	echo "Pruning revision $revision:"
	(time duplicacy prune -id benchmark -r "$revision" -collect-only -threads 1) 2>&1
	# perform empty backup to allow fossil deletion
	sleep 1
	duplicacy backup
	(time duplicacy prune -delete-only -threads 1) 2>&1
done
rm "$repository/empty"
