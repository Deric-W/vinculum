# vinculum

![CI](https://github.com/Deric-W/vinculum/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/Deric-W/vinculum/graph/badge.svg?token=mEPwkHIezV)](https://codecov.io/gh/Deric-W/vinculum)

Implementation of Lock-Free Deduplication in Rust.

This crates provides an implementation of the algorithm used by tools
such as [Duplicacy](https://duplicacy.com) as described in their
[paper](https://github.com/gilbertchen/duplicacy/blob/master/duplicacy_paper.pdf).

## Overview

The most important object is the repository, which stores chunks, clients
and manifests and is available to a number of clients.
It is represented by the `Repository` trait, which is designed to be
implemented by you.

Clients represent individual users which may perform operations on the
repository at the same time as other clients, like creating chunks or
manifests.
A single client can only perform some operations like manifest creation
sequentially, while multiple clients can perform them in parallel.
Furthermore, clients have to be registered with the repository before
they perform any operations and should be removed when they won't create
new manifests for a long time.

Manifests represent data uploaded by a client which has been divided
into chunks and stored in the repository.
Manifests only store references to their chunks and may share them with
other manifests, which requires periodic chunk removals after some manifests
have been deleted.

Chunks are pieces of data uploaded to the repository and may be referenced
by manifests.
They allow for deduplicating manifest contents by associating them with an
id based on their content, causing chunks with the same content to only be
stored once in the repository.

Chunk creation can be done in parallel (even by a single client), but chunk
removal is more complicated because other clients can be in the process of
creating manifests referencing them, which would cause invalid references
should these chunks be deleted.
To prevent this chunks should be first turned into a special type of chunk
called "fossils" by using `FossilCollectionBuilder`, which produces a
`FossilCollection`.
This collection can be deleted when every client has created a manifest
after the fossil collection was created, which will either permanently
delete fossils or turn them back into chunks.
It is possible to combine the deletion of a fossil collection with the
creation of the next one using `PipelinedFossilCollectionBuilder`.

## Concurrency

While this crate does not depend on a particular async runtime it performs
actions concurrently when possible, which can be controlled by the `concurrency`
argument of some functions.

This is achieved by using `futures::stream::FuturesUnordered`, which in
turn requires that any blocking operations (for example filesystem operations
or hashing large amounts of data) are queued on an external thread pool should
parallel execution be desired.

## Features

- `serde`: implements `serde::Serialize` and `serde::Deserialize` for `FossilCollection`.
