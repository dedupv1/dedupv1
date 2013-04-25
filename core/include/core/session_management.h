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

#ifndef SESSION_MANAGEMENT_H__
#define SESSION_MANAGEMENT_H__

#include <core/dedup.h>
#include <base/locks.h>

#include <base/resource_management.h>
#include <string>

namespace dedupv1 {

class ContentStorage;
class Session;
class DedupVolume;

class SessionResourceType : public dedupv1::base::ResourceType<Session> {
    private:
    DISALLOW_COPY_AND_ASSIGN(SessionResourceType);
    DedupVolume* volume_;
    public:
    explicit SessionResourceType(DedupVolume* volume);
    ~SessionResourceType();

    virtual Session* Create();
    virtual void Reinit(Session* sess);
    virtual void Close(Session* sess);
};

}

#endif  // SESSION_MANAGEMENT_H__
