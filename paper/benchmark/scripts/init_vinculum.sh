#!/usr/bin/env sh
set -e

# Create a Vinculum repository and populate it with manifests created from a set of dataset files.
repository="$1"
shift
echo "(re)creating Vinculum repository in $repository ..."
rm -rf "$repository"
vinculum-benchmark init "$repository"
vinculum-benchmark add-client "$repository" benchmark

echo "Creating manifests for datasets $* ..."
for dataset in "$@"
do
    vinculum-benchmark create "$repository" "$(basename "$dataset")" benchmark "$dataset" --chunk-size 1048576
done
