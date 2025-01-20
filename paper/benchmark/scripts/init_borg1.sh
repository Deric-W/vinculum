#!/usr/bin/env sh
set -e

# Create a Borg 1 repository and populate it with manifests created from a set of dataset files.

repository="$1"
shift
echo "(re)creating $(borg --version) repository in $repository ..."
rm -rf "$repository"
borg init --encryption=none "$repository"

echo "Creating manifests for datasets $* ..."
for dataset in "$@"
do
    borg create --compression=none --stats --chunker-params=fixed,1048576 "$repository::$(basename "$dataset")" "$dataset"
done
