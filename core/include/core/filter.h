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
#ifndef FILTER_H__
#define FILTER_H__

#include <core/dedup.h>
#include <core/chunk_mapping.h>
#include <core/block_mapping.h>
#include <core/session.h>
#include <base/factory.h>
#include <base/error.h>
#include <tbb/atomic.h>

#include <map>
#include <string>

namespace dedupv1 {

class DedupSystem;

/**
 * \namespace dedupv1::filter
 * Namespace for classes related filter and the filter chains
 */
namespace filter {

/**
 * \ingroup filterchain
 *
 * Abstract base class for all filter implementations.
 *
 */
class Filter : public dedupv1::StatisticProvider {
private:
    DISALLOW_COPY_AND_ASSIGN(Filter);
public:
    /**
     * \ingroup filterchain
     * Enumerations about the possible result of an
     * filter check.
     */
    enum filter_result {
        /**
         * The filter is absolutely sure that the chunk is known.
         * The data address of the chunk in the chunk mapping has to be set.
         *
         * The only filter that can return this value is the ByteCompareFilter that
         * performs an exact comparison.
         */
        FILTER_EXISTING = 4,

        /**
         * The filter states the the chunk is known with very high probability (usually
         * higher than the error rates of the hardware). The standard case of this is
         * that the chunk fingerprint is stored in the ChunkIndex.
         *
         * The data address of the chunk has to be set after a Check with this result.
         *
         * After such a result only filter are executed, that allow an EXISTING result.
         * Filter that can only return a STRONG_MAYBE, aren't executed anymore.
         */
        FILTER_STRONG_MAYBE = 3,

        /**
         * The filter doesn't state that the chunk is a duplicate nor does it
         * state that it cannot be a duplicate. The filter doesn't know. So other filters
         * must be executed.
         */
        FILTER_WEAK_MAYBE = 2,

        /**
         * The filter is sure that the chunk is not known. This can
         * be the result of a negative ChunkIndex check. The other filters
         * aren't executed anymore.
         */
        FILTER_NOT_EXISTING = 1,

        /**
         * An error happened during the filter check.
         */
        FILTER_ERROR = 0,
    };
private:
    /**
     * \ingroup filterchain
     *
     * Statistics about the filter chain
     */
    struct filter_statistics {
        tbb::atomic<uint64_t> checks_;
        tbb::atomic<uint64_t> not_existing_count_;
        tbb::atomic<uint64_t> maybe_count_;
        tbb::atomic<uint64_t> existing_count_;
        tbb::atomic<uint64_t> updates_;
    };

    /**
     * Maximal filter level that a filter can "produce"
     */
    enum filter_result max_filter_level_;

    /**
     * Name of the filter
     */
    std::string name_;

    /**
     * true iff the filter is enabled by default
     */
    bool enabled_by_default_;

    /**
     * Factory for filter instances
     */
    static MetaFactory<Filter> factory_;
public:

    static MetaFactory<Filter>& Factory();
    /**
     * Constructs a new filter.
     *
     * The concrete class should set the max_filter_level to the
     * maximal filter type, that the filter can return.
     *
     * @param name name of the filter type
     * @param max_filter_level
     * @return
     */
    Filter(const std::string& name, enum filter_result max_filter_level);

    /**
     * Destructor
     */
    virtual ~Filter();

    /**
     * Inits a new filter. This method should be
     * called directly after the creation of the object.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Init();

    /**
     * Configures the filter.
     *
     * The supported options depend on the concrete filter.
     *
     * Available options:
     * - enabled: Boolean
     *
     * @param option_name
     * @param option
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool SetOption(const std::string& option_name, const std::string& option);

    /**
     * Starts the filter.
     *
     * The default implementation simply returns true.
     * @param system
     * @param start_context
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Start(DedupSystem* system);

    /**
     * This method checks if the given chunk is known or a new chunk.
     *
     * The filter can use all information accessible through the dedup system
     * (see Start method), all filter local data, the current session, the current
     * block mapping, and the current chunk.
     *
     * The filter check returns only of the 5 defined filter results. If a filter
     * returns a STRONG_MAYBE result or higher, it should set the data_address
     * member of the chunk mapping. For the semantics of the filter results,
     * the the enum filter_result documentation.
     *
     * @param session
     * @param block_mapping current block mapping, can be NULL
     * @param mapping
     * @param ec
     * @return
     */
    virtual enum filter_result Check(
        dedupv1::Session* session,
        const dedupv1::blockindex::BlockMapping* block_mapping,
        dedupv1::chunkindex::ChunkMapping* mapping,
        dedupv1::base::ErrorContext* ec) = 0;

    /**
     * This method is called after the filter result of the complete filter chain
     * as been processed. If the chunk is a new chunk, the storage component is called
     * before the Update method. The update method is only called for new chunks.
     *
     * In this method, the filter implementations can update their internal data structures.
     *
     * @param session
     * @param mapping
     * @param ec
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Update(dedupv1::Session* session,
        const dedupv1::blockindex::BlockMapping* block_mapping,
        dedupv1::chunkindex::ChunkMapping* mapping,
        dedupv1::base::ErrorContext* ec);

    virtual bool UpdateKnownChunk(dedupv1::Session* session,
        const dedupv1::blockindex::BlockMapping* block_mapping,
        dedupv1::chunkindex::ChunkMapping* mapping,
        dedupv1::base::ErrorContext* ec);

    /**
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Abort(dedupv1::Session* session,
          const dedupv1::blockindex::BlockMapping* block_mapping,
          dedupv1::chunkindex::ChunkMapping* chunk_mapping,
          dedupv1::base::ErrorContext* ec);

    /**
     * Closes the filter and frees all its resources.
     * @return true iff ok, otherwise an error has occurred
     */
    virtual bool Close();

    inline int GetMaxFilterLevel() const;

    /**
     * returns the name of the filter
     */
    inline const std::string& GetName() const;

    static std::string GetFilterResultName(enum filter_result fr);

    /**
     * returns true iff the filter is enabled by default.
     */
    inline bool is_enabled_by_default() const {
        return enabled_by_default_;
    }
};

int Filter::GetMaxFilterLevel() const {
    return this->max_filter_level_;
}

const std::string& Filter::GetName() const {
    return this->name_;
}

}
}

#endif  // FILTER_H__
