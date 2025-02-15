#!/bin/sh

# Generate the results of the manifest and chunk benchmarks.

set -e
location="$1"
borg1="$2"
borg2="$3"
shift 3

mkdir /tmp/thesis

for chunksize in 64 512 4096 8192 16384 32768 65536 131072 262144 524288
do
    ./scripts/generate_csv.py \
        --borg1 "$borg1" \
        --borg2 "$borg2" \
        -o "results/chunks${chunksize}.csv" \
        -a /tmp/thesis \
        --chunk-size "$chunksize" \
        --chunks-start 0 \
        --chunks-end 20001 \
        --chunks-step 4000 \
        --manifests-start 4 \
        --manifests-end 5 \
        --manifests-step 1 \
        "$location"
done

./scripts/generate_csv.py \
    --borg1 "$borg1" \
    --borg2 "$borg2" \
    -o "results/manifests0const.csv" \
    -a /tmp/thesis \
    --chunk-size 4096 \
    --chunks-start 0 \
    --chunks-end 1 \
    --chunks-step 1 \
    --manifests-start 2 \
    --manifests-end 1003 \
    --manifests-step 100 \
    "$location"

./scripts/generate_csv.py \
    --borg1 "$borg1" \
    --borg2 "$borg2" \
    -o "results/manifests1const.csv" \
    -a /tmp/thesis \
    --chunk-size 4096 \
    --chunks-start 1 \
    --chunks-end 2 \
    --chunks-step 1 \
    --manifests-start 2 \
    --manifests-end 1003 \
    --manifests-step 100 \
    "$location"

./scripts/generate_csv.py \
    --borg1 "$borg1" \
    --borg2 "$borg2" \
    -o "results/manifests1000const.csv" \
    -a /tmp/thesis \
    --chunk-size 4096 \
    --chunks-start 1000 \
    --chunks-end 1001 \
    --chunks-step 1 \
    --manifests-start 2 \
    --manifests-end 1003 \
    --manifests-step 100 \
    "$location"

rmdir /tmp/thesis
