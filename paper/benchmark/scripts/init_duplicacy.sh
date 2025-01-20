#!/usr/bin/env sh
set -e

# Create a Duplicacy repository and storage and populate it with manifests created from a set of dataset files.

repository="$1"
storage="$2"
shift 2
echo "(re)creating Duplicacy repository in $repository and storage in $storage ..."
rm -rf .duplicacy "$repository" "$storage"
mkdir "$repository"
duplicacy init -repository "$repository" -c 1048576 -min 1048576 -max 1048576 benchmark "$(realpath "$storage")"
# set compression level to passthrough and error if pattern not found
sed -i '/"compression-level": 100/,${s//"compression-level": 0/;b};$q1' "$storage/config"

echo "Creating manifests for datasets $* ..."
for dataset in "$@"
do
    ln "$dataset" "$repository"
    duplicacy backup
    rm -rf "${repository:?}/"*
done

duplicacy list -a -files
