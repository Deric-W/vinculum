#!/usr/bin/env sh
set -e

# Create a Borg 2 repository and populate it with manifests created from a set of dataset files.

repository="$1"
shift
echo "(re)creating $(borg --version) repository in $repository ..."
rm -rf "$repository"
borg repo-create --repo "$repository" --encryption=none

echo "Creating manifests for datasets $* ..."
for dataset in "$@"
do
    borg create --repo "$repository" --compression=none --stats --chunker-params=fixed,1048576 "$(basename "$dataset")" "$dataset"
done
