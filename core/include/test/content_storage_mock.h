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

#ifndef CONTENT_STORAGE_MOCK_H_
#define CONTENT_STORAGE_MOCK_H_

#include <gmock/gmock.h>
#include <core/dedup_system.h>
#include <core/content_storage.h>
#include <base/option.h>

class MockContentStorage : public dedupv1::ContentStorage {
    public:
    MOCK_METHOD1(GetFilterList, dedupv1::base::Option<std::list<dedupv1::filter::Filter*> >(
            const std::set<std::string>& enabled_filter_names));
    MOCK_METHOD0(default_chunker, dedupv1::Chunker*());
};

#endif /* CONTENT_STORAGE_MOCK_H_ */
