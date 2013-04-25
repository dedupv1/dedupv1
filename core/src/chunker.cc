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

#include <core/chunker.h>
#include <base/logging.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <base/timer.h>

using std::map;
using std::string;
using std::list;

LOGGER("Chunker");

namespace dedupv1 {

MetaFactory<Chunker> Chunker::factory_("Chunker", "chunker");

MetaFactory<Chunker>& Chunker::Factory() {
    return factory_;
}

bool Chunker::SetOption(const string& option_name, const string& option) {
    ERROR("Illegal option " << option_name);
    return false;
}

Chunker::Chunker() {
}

Chunker::~Chunker() {
}

ChunkerSession::ChunkerSession() {
}

ChunkerSession::~ChunkerSession() {
}

bool ChunkerSession::Clear() {
    return true;
}

}
