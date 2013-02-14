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

#ifndef FILTER_MOCK_H_
#define FILTER_MOCK_H_

#include <gmock/gmock.h>
#include <core/filter.h>

class MockFilter : public dedupv1::filter::Filter {
public:
    MockFilter(const std::string& name, dedupv1::filter::Filter::filter_result max_filter_level) :
        dedupv1::filter::Filter(name, max_filter_level) {
    }

    MOCK_METHOD4(Check, dedupv1::filter::Filter::filter_result(
                dedupv1::Session* session,
                const dedupv1::blockindex::BlockMapping* block_mapping,
                dedupv1::chunkindex::ChunkMapping* mapping,
                dedupv1::base::ErrorContext* ec));

    MOCK_METHOD3(Update, bool(dedupv1::Session* session,
                dedupv1::chunkindex::ChunkMapping* mapping,
                dedupv1::base::ErrorContext* ec));

    MOCK_METHOD3(Abort, bool(dedupv1::Session* session, dedupv1::chunkindex::ChunkMapping* chunk_mapping,
                dedupv1::base::ErrorContext* ec));


};

#endif /* FILTER_MOCK_H_ */
