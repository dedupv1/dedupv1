/*
 * dedupv1 - iSCSI based Deduplication System for Linux
 *
 * (C) 2008-2009 Paderborn Center for Parallel Computing
 */

#ifndef BLOOM_FILTER_H__
#define BLOOM_FILTER_H__

#include <tbb/atomic.h>

#include <core/dedup.h>
#include <base/locks.h>
#include <core/filter.h>
#include <core/log_consumer.h>
#include <core/container_tracker.h>
#include <base/profile.h>
#include <base/fileutil.h>
#include <core/container_storage.h>
#include <core/log.h>
#include <base/bloom_set.h>
#include <string>
#include <set>

namespace dedupv1 {
namespace filter {

/**
* A bloom filter is a probabilistic data structure with set like operations.
* It allows to add items and to test for membership. However, the membership test 
* operations are special. If the membership test failed, we can be sure that the 
* search key is stored in the bloom filter. If the membership test succeeds, 
* there is a small probability that the key isn't in the set, too.
*
* After n inserted object, a bloom filter with k hash functions and m bits of RAM
* returns with a probability of (1−(1− 1 )kn)k m a false positive answer. This 
* means that the bloom filter states that the key is member of the set, but is 
* is actually not the case. We use a bloom filter because it is a much more compact 
* representation of the index data.
*
* In a deduplication system, an obvious way to reduce index accesses is the use 
* a bloom filter before the ChunkIndexFilter. Here, we insert every new 
* fingerprint into the bloom filter and test the bloom filter for every fingerprint 
* during the Lookup. If the fingerprint is not known, we can be sure that we 
* haven't stored the fingerprint yet ( NOT_EXISTING). If the fingerprint seems 
* to be known, we return A WEAK_MAYBE, because we cannot be sure with a very 
* high probability (and we cannot set a data address).
*
* We have to make sure that the bloom filter works correct even after restarts. 
* So we backup the bloom filter to disk.
*
* NOTE: Currently the bloom filter never deletes data or is refreshed.
* Therefore the filter has an increasing false positive rate when chunks are 
* garbage collected.
*
* IMPORTANT NOTE: The bloom filter is currently not crash-safe as it overwrites
* the file which might lead to inconsistent on-disk state and the bloom filter
* is currently not recovering the chunk index state after a crash.
*
* Bloom filters are developed by Bloom and published in "B. H. Bloom. Space/time 
* trade-offs  in hash coding with allowable errors. Communications of the ACM, 1970.".
* In the context of deduplication, a similar filter is used at least in some
* version of the Venti system and it is also proposed in "B. Zhu, K. Li, and 
* H. Patterson. Avoiding the disk bottleneck in the data domain deduplication 
* file system. In 6th Usenix Conference on File and Storage Technologies, 
* pages 269–282, February 2008.".
*
* \ingroup filterchain
*/
class BloomFilter: public Filter {
    private:
    DISALLOW_COPY_AND_ASSIGN(BloomFilter);

    /**
    * Statistics about the bloom filter
    */
    class Statistics {
      public:
        Statistics();

        tbb::atomic<uint64_t> reads_;
        tbb::atomic<uint64_t> writes_;
        tbb::atomic<uint64_t> weak_hits_;
        tbb::atomic<uint64_t> miss_;

        dedupv1::base::Profile time_;
    };

    /**
    * Bloom set
    */
    dedupv1::base::BloomSet* bloom_set_;

    /**
    * Size of the bloom filter in estimated number of entries
    */
    uint64_t size_;

    /**
    * Name of the filter filename
    */
    std::string filter_filename_;

    /**
    * File for the filer data
    */
    dedupv1::base::File* filter_file_;

    /**
    * Statistics about the bloom filter
    */
    Statistics stats_;

    bool ReadData();
    bool DumpData();
    public:

    /**
     * Constructor
     * @return
     */
    BloomFilter();

    /**
     * Destructor
     * @return
     */
    virtual ~BloomFilter();

    /**
     * Closes the bloom filter and frees all its resources
     * @return
     */
    virtual bool Close();

    /**
    * Configures the bloom filter.
    *
    * Possible configuration values are:
    * - "size": size (in entries or bits) of the in-memory bloom filter array. The
    *   value can be given with a size prefix, e.g. 2M for 2^21 entries.
    * - "k": Number of hash functions.
    * - "filter-filename": Name of the file where we store the bloom filter persistently
    *
    * @param option_name
    * @param option
    * @return
    */
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    /**
    * Starts the bloom filter.
    * @param system
    * @param start_context
    * @return
    */
    virtual bool Start(DedupSystem* system);

    /**
    * Checks of the given chunk (mapping) is a known chunk.
    *
    * If the bloom filter rejects the fingerprint, the filter result
    * NOT_EXISTING is returned. If the bloom filter can not reject the
    * fingerprint WEAK_MAYBE is returned.
    *
    * @param session
    * @param block_mapping
    * @param mapping
    * @return
    */
    virtual enum filter_result Check(
            dedupv1::Session* session,
            const dedupv1::blockindex::BlockMapping* block_mapping,
            dedupv1::chunkindex::ChunkMapping* mapping,
            dedupv1::base::ErrorContext* ec);

    /**
    * Updates the bloom filter with the new chunk.
    *
    * @param session
    * @param mapping
    * @return
    */
    virtual bool Update(dedupv1::Session* session, 
        const dedupv1::blockindex::BlockMapping* block_mapping,
        dedupv1::chunkindex::ChunkMapping* mapping,
        dedupv1::base::ErrorContext* ec);

    virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

    /**
    * Prints statistics about the bloom filter
    * @return
    */
    virtual std::string PrintStatistics();

    /**
    * Prints profile information
    * @return
    */
    virtual std::string PrintProfile();

    /**
    * Creates a new bloom filter.
    * @return
    */
    static Filter* CreateFilter();

    /**
    * Registers the bloom filter at the filter factory.
    */
    static void RegisterFilter();
};

}
}

#endif  // BLOOM_FILTER_H__
