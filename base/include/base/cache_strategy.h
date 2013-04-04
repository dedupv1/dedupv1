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
#ifndef CACHE_STRATEGY_H__
#define CACHE_STRATEGY_H__

#include <list>
#include <map>
#include <tbb/tick_count.h>

namespace dedupv1 {
namespace base {

/**
 * Cache strategy.
 * The cache strategy does not perform the caching itself it does only
 * inform the client about which item to remove (usually the index of
 * an vector).
 *
 * The cache strategies are not thread-safe. They are strategies, not
 * full cache implementations
 */
template<class T> class CacheStrategy {
    public:
        /**
         * Constructor.
         * @return
         */
        CacheStrategy() {
        }

        /**
         * Destructor
         * @return
         */
        virtual ~CacheStrategy() {
        }

        /**
         * Called when an item with a given index is used (or inserted).
         *
         * @param o Object to touch
         * @return
         */
        virtual bool Touch(T o) = 0;

        /**
         * Called when an item must be replaces. The
         * out parameter o denotes the index of the item to replace.
         * @param o
         * @return
         */
        virtual bool Replace(T* o) = 0;

        /**
         * Deltes an object from the cache
         */
        virtual bool Delete(T o) = 0;

        /**
         * Returns the current size of the cache.
         * @return
         */
        virtual size_t size() = 0;
};

/**
 * Least-Recently used cache strategy.
 */
template<class T> class LRUCacheStrategy : public CacheStrategy<T> {
    private:
    /**
     * Stores the ordering of accesses to the object.
     * New objects are added to the front and the last element is
     * replaced when necessary.
     */
    typename std::list<T> objects_;

    /**
     * Map that stores the data and a pointer to the position in the list
     */
    typename std::map<T, typename std::list<T>::iterator > object_map_;
    public:
    /**
     * Constructor
     * @return
     */
    LRUCacheStrategy() {
    }

    /**
     * Destructor
     * @return
     */
    virtual ~LRUCacheStrategy() {
    }

    /**
     * Returns the cached objects in the order of the most recent access.
     */ 
    const std::list<T>& ordered_objects() const {
        return objects_;
    }

    virtual bool Touch(T o) {
        typename std::map<T, typename std::list<T>::iterator >::iterator i = object_map_.find(o);

        if (i != object_map_.end()) {
            // This is a nice trick to move an element to the front, O(1)
            objects_.splice(objects_.begin(), objects_, i->second);
            i->second = objects_.begin();
        } else {
            objects_.push_front(o);
            object_map_[o] = objects_.begin();
        }
        return true;
    }

	/**
	 * Delete an object from the LRU cache
	 */
    virtual bool Delete(T o) {
        if (objects_.empty()) {
            return true;
        }
        typename std::map<T, typename std::list<T>::iterator >::iterator i = object_map_.find(o);
        if (i == object_map_.end()) {
            return true;
        }
        objects_.erase(i->second);
        object_map_.erase(i);
        return true;
    }

    virtual bool Replace(T* o) {
        if(!o) return false;

        if (objects_.empty()) {
            return false;
        }
        typename std::list<T>::iterator i = --objects_.end(); // last element
        T tmp = *i;
        objects_.erase(i);

        typename std::map<T, typename std::list<T>::iterator >::iterator j = object_map_.find(tmp);
        if (j != object_map_.end()) {
            object_map_.erase(j);
        }
        *o = tmp;
        return true;
    }

    /**
     * returns the number of objects in the cache
     */
    virtual size_t size() {
        return objects_.size();
    }
};

/**
 * A set in which all elements that are older than x seconds are removed.
 *
 * Not multi threaded.
 */
template<class T> class TimeEvictionSet {
    private:
        typename std::list<T> objects_;
        typename std::map<T, typename std::list<T>::iterator > object_map_;
        typename std::map<T, tbb::tick_count > object_time_map_;
        uint32_t seconds_;

		/**
		 * Removes all expired items from the set
		 */
        void Purne() {
            if (objects_.size() == 0) {
                return;
            }
            tbb::tick_count now = tbb::tick_count::now();

            while(objects_.size() > 0) {
                typename std::list<T>::iterator i = --(objects_.end()); // last element
                tbb::tick_count last_touch_count = object_time_map_[*i];
                if ((now - last_touch_count).seconds() > seconds_ ) {
                    typename std::map<T, typename std::list<T>::iterator >::iterator j = object_map_.find(*i);
                    if (j != object_map_.end()) {
                        object_map_.erase(j);
                    }
                    typename std::map<T, tbb::tick_count >::iterator k = object_time_map_.find(*i);
                    if (k != object_time_map_.end()) {
                        object_time_map_.erase(k);
                    }
                    objects_.erase(i);
                } else {
                    // this and all elements before are valid
                    return;
                }
            }
        }

    public:
        /**
         * Constructor
         * @param seconds
         * @return
         */
        TimeEvictionSet(uint32_t seconds) {
            this->seconds_ = seconds;
        }

        /**
         * Sets the eviction time.
         * Automatically purnes the set.
         * @param seconds
         */
        void SetSeconds(uint32_t seconds) {
            this->seconds_ = seconds;
            this->Purne();
        }

        /**
         * Destructor.
         * @return
         */
        virtual ~TimeEvictionSet() {
        }

        /**
         * Inserts a new element.
         * If the element was also in the set, the element is "re-newed".
         * @param o
         */
        virtual void Insert(const T& o) {
            typename std::map<T, typename std::list<T>::iterator >::iterator i = object_map_.find(o);

            if (i != object_map_.end()) {
                // This is a nice trick to move an element to the front, O(1)
                objects_.splice(objects_.begin(), objects_, i->second);
                i->second = objects_.begin();
            } else {
                objects_.push_front(o);
                object_map_[o] = objects_.begin();
            }
            object_time_map_[o] = tbb::tick_count::now();
        }

        /**
         * Checks if the element is in the set.
         * Automatically purnes the set.
         * @param o
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Contains(const T& o) {
            if (objects_.empty()) {
                return false;
            }
            Purne();
            return object_map_.find(o) != object_map_.end();

        }

        /**
         * Returns the size of the set
         * @return
         */
        virtual size_t size() {
            return objects_.size();
        }


};

}
}

#endif  // CACHE_STRATEGY_H__
