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

#include <base/sliding_average.h>
#include <base/logging.h>

using std::map;

LOGGER("SlidingAverage");

namespace dedupv1 {
namespace base {

SlidingAverage::SlidingAverage(int window_size) {
    this->window_size = window_size;
    this->sum = 0;
}

bool SlidingAverage::Add(int key, double value) {
    TRACE("Add value: key " << key << ", value " << value);
    // remove to old data
    bool delete_some = false;
    map<int, double>::iterator first;
    map<int, double>::iterator last;
    for (map<int, double>::iterator i = this->data.begin(); i != this->data.end(); i++) {
        TRACE("Check window: current key " << key << ", check key " << i->first << ", check value " << i->second);
        if (key - i->first >= this->window_size) {
            if (i == this->data.begin()) {
                first = i;
                delete_some = true;
            }
            last = i;
            this->sum -= i->second;
            DEBUG("Remove out of window entry: key " << i->first << ", value " << i->second);
        } else {
            break;
        }
    }
    if (delete_some) {
        this->data.erase(first, ++last);
    }
    this->data[key] += value;
    this->sum += value;
    return true;
}

double SlidingAverage::GetAverage(int current_key) {
    // remove to old data
    bool delete_some = false;
    map<int, double>::iterator first;
    map<int, double>::iterator last;
    for (map<int, double>::iterator i = this->data.begin(); i != this->data.end(); i++) {
        TRACE("Check window: current key " << current_key << ", check key " << i->first << ", check value " << i->second);
        if (current_key - i->first >= this->window_size) {
            if (i == this->data.begin()) {
                first = i;
                delete_some = true;
            }
            last = i;
            this->sum -= i->second;
            TRACE("Remove out of window entry: key " << i->first << ", value " << i->second);
        } else {
            break;
        }
    }
    if (delete_some) {
        this->data.erase(first, ++last);
    }

    double a = this->sum / this->window_size;
    TRACE("sum " << this->sum << ", window " << this->window_size << ", average " << a);
    return a;
}

}

}
