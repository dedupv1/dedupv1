/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008 Dirk Meister
 * (C) 2009 - 2011, Dirk Meister, Paderborn Center for Parallel Computing
 * (C) 2012 Dirk Meister, Johannes Gutenberg University Mainz
 *
 * This file is part of dedupv1.
 *
 * dedupv1 is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * dedupv1 is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with dedupv1. If not, see http://www.gnu.org/licenses/.
 */

#ifndef DEDUP_H__
#define DEDUP_H__

#include <base/base.h>

/**
 * \mainpage dedupv1 data deduplication system
 *
 * This documentation describes the dedupv1 data deduplication system.
 * Data deduplication systems discover redundancies between different data blocks.
 * These redundancies are removed to reduce storage capacity. The limited random IO
 * performance of hard disks limits the overall throughput of such systems. The current target
 * environment is based on solid state technology.
 *
 * The architecture of the system is shown in Figure:
 *
 * \image html docs/architecture.jpg "Architecture of dedupv1"
 * \image latex docs/architecture.eps "Architecture of dedupv1"
 *
 * The system is integrated in the the generic SCSI target system (SCST) via an user-level target
 * extension. The SCST supports different types of storage backends, which allowed us to
 * integrate our deduplication system as such a backend.
 * The data deduplication is therefore transparent to the user of the iSCSI target.
 *
 * The ChunkIndex stores all known chunk fingerprints. The key of the index
 * is a SHA-1 fingerprint (20 byte) of the chunk and the value is the storage
 * address with that the data of the chunk can be read from the storage component.
 * We implemented an in-memory chained hash table and a static, paged disk-based hash table
 * to store the chunk index. Addtinally a B-Tree can be used, but this doesn't seem to be a good fit because
 * of the lack of key-space locality.
 *
 * The BlockIndex stores the metadata that is necessary to map the blocks of the
 * iSCSI target to the chunks of varying length. We call the mapping of a block the BlockMapping.
 *
 * The FilterChain detects whether a chunk is a duplicate. A series of different filters can
 * be executed and the result of the steps determines the further execution. Each filter step
 * returns with one of the following results:
 * - \c EXISTING: The current chunk is a duplicate, e.g. the filter has performed a  byte-wise comparison.
 * - \c ESTRONG-MAYBE: The current chunk is a duplicate with very high probability. This is the case after
 *   a fingerprint comparison. Only filters that can deliver \c EXISTING should  be executed afterwards.
 * - \c WEAK-MAYBE: The filter cannot make any statement about the duplication state of the chunk.
 * - \c NOT-EXISTING: The filter rules out the possibility that the chunk is already known, e.g. after
 *   a Chunk Index lookup with a negative result.
 *
 * When a new chunk is found, the filter chain is executed a second time so that filters can
 * update their internal state.
 *
 * This flexible duplicate detection abstraction allows developing and evaluating new approaches
 * for duplicate detection with minimal implementation effort. Some filters are:
 * - ChunkIndexFilter: The Chunk Index Filter is the basic filter for data deduplication.
 *   It checks for each chunk, whether the fingerprint of the chunk is already stored in the
 *   ChunkIndex.
 * - BlockIndexFilter: The Block Index Filter (BIF) checks the current chunk against the
 *   block mapping of the currently written block that is already present in main memory. If the
 *   same chunk is written to the same block as in the previous run, the BIF is able to avoid the
 *   chunk index lookup. In a backup scenario, we clone the block mappings of the previous backup
 *   using a server-side approach to a location that will hold the new backup data. So the
 *   current backup can be seen as overwriting the previous backup, which sometimes enables
 *   the BIF to avoid IO load.
 * - ByteCompareFilter: The Byte Compare Filter (BCF) performs an exact byte-wise comparison
 *   of the current chunk and the already stored chunk with the same fingerprint. While this
 *   introduces load on the storage systems, it also eliminates the possibility of unnoticed hash
 *   collisions.
 *
 * The chunk data is stored using a subsystem called ChunkStore. The chunk store collects
 * chunk data until a container of a specific size (often 4 MB) is filled up and then writes
 * the complete container to disk.
 */

/**
 * \page literature Related Work
 *
 * - B. Zhu, K. Li, and H. Patterson, "Avoiding the disk bottleneck in the data domain
 *   deduplication file system," in 6th Usenix Conference on File and Storage Technologies,
 *   February 2008
 */

/**
 * \page life_cycle Life Cicle of long running Objects
 *
 * Most long-living objects follow the same object lifecycle protocol that makes the usage and
 * development of these objects easier. The following stages are later described in more detail:
 *
 * - Init
 * - Start
 * - Run (Only if the object starts own threads)
 * - Stop (Only if the object starts own threads)
 *
 * \section Init
 *
 * Directly after the creation of an lifecycle object, the Init method should be called if available.
 * As noted in the code style, only simple assignments should be done in the Constructor. Everything
 * that can fail should be done in an Init method.
 *
 * Every configuration using the method SetOption should be done after the Init call and before the
 * start stage.
 *
 * Valid transitions are to "Start", and "Stop". If the Init method fails, the transitions
 * to Start and Run are not valid.
 *
 * \section Start
 *
 * Valid transitions are to "Run", and"Stop". If the Start() method fails, the transition
 * to Run is not valid.
 *
 * \section Run
 *
 * The "Run" method marks the transition of the start stage to the running stage. In the Run method
 * usually all threads are started.
 *
 * Valid transitions are to "Stop"
 *
 * \section Stop
 *
 * The "Stop" method usually marks the transition from running stage to stopped state. However, Stop
 * can also be called from stages before running. During the Stop method, usually all threads started
 * by an object and its child objects are stopped (joined).
 *
 * The only valid transition is to delete the object.
 *
 */

#endif  // DEDUP_H__
