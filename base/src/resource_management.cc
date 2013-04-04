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

#include <base/resource_management.h>
#include <base/aprutil.h>
#include <base/logging.h>
#include <base/strutil.h>

#include <stdio.h>
#include <string.h>

using std::string;
using dedupv1::base::apr::apr_error;

LOGGER("ResourceManagement");

namespace dedupv1 {
namespace base {
namespace internal {

ResourceManagementImpl::ResourceManagementImpl() {
    pool = NULL;
    resource_list = NULL;
    max = 0;
}

int ResourceManagementImpl::GetAcquiredCount() {
    if (!this->resource_list) {
        return 0;
    }
    return apr_reslist_acquired_count(this->resource_list);
}

ResourceManagementImpl::~ResourceManagementImpl() {
    if (this->resource_list) {
        int count = apr_reslist_acquired_count(this->resource_list);
        if (count > 0) {
            WARNING("Failed to destroy resource management: " << name
                                                              << ", open resources " << count);
            // A memory leak is better than a crash
            this->resource_list = NULL;
            this->pool = NULL;
            return;
        }
        apr_reslist_destroy(this->resource_list);
        this->resource_list = NULL;
    }
    if (this->pool) {
        apr_pool_destroy(this->pool);
        this->pool = NULL;
    }
}

bool ResourceManagementImpl::Init(const string& name, apr_reslist_constructor con, apr_reslist_destructor de, void* rm, int maximal_size, bool enforce_max_size) {
    apr_status_t status = 0;

    this->name = name;
    this->max = maximal_size;
    this->resource_list = NULL;
    enforce_max_size_ = enforce_max_size;
    apr_interval_time_t ttl = 0;

    status = apr_pool_create_core(&this->pool);
    if (status != 0) {
        return false;
    }

    status = apr_reslist_create(&this->resource_list, 0,
        maximal_size, maximal_size, ttl,
        con, de, rm, this->pool);
    if (status != 0) {
        return false;
    }
    return true;
}

void* ResourceManagementImpl::Acquire() {
    int count = apr_reslist_acquired_count(this->resource_list);
    if (count >= this->max && enforce_max_size_) {
        WARNING("max acquired count reached: " << this->name <<
            ", max count " << this->max <<
            ", currently acquired count " << count);
        return NULL;
    }

    void* resource = NULL;
    apr_status_t status = apr_reslist_acquire(this->resource_list, (void * *) &resource);
    if (status != 0) {
        // we have seen instances where the message was
        // "There is no error, this value signifies an initialized error code" // NOLINT
        // this is highly misleading. Usually this means that the Init of a
        // resource type failed

        ERROR("Failed to acquire resource: message " << apr_error(status) <<
            ", name " << this->name <<
            ", max count " << this->max <<
            ", currently acquired count " << count);
        return NULL;
    }
    return resource;
}

bool ResourceManagementImpl::Release(void* resource) {
    if (!resource) {
        return true;
    }
    apr_status_t status = apr_reslist_release(this->resource_list, resource);
    if (status != 0) {
        return false;
    }
    return true;
}

} // end internal

}
}

