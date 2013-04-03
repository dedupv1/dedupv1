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
#ifndef FILTER_CHAIN_TEST_UTIL_H_
#define FILTER_CHAIN_TEST_UTIL_H_

#include <test/filter_chain_test_util.h>
#include <core/filter_chain.h>
#include <base/logging.h>

LOGGER("FilterChainTestUtil");

using dedupv1::filter::FilterChain;

namespace dedupv1 {
namespace testutil {
dedupv1::filter::FilterChain* CreateDefaultFilterChain() {
    FilterChain* filter_chain = new FilterChain();
    CHECK_RETURN(filter_chain, NULL, "Failed to create filter chain");

    CHECK_RETURN(filter_chain->AddFilter("block-index-filter"), NULL, "Failed to create filter chain");
    CHECK_RETURN(filter_chain->AddFilter("chunk-index-filter"), NULL, "Failed to create filter chain");
    return filter_chain;
}
}
}

#endif /* FILTER_CHAIN_TEST_UTIL_H_ */
