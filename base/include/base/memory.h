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

#ifndef MEMORY_H__
#define MEMORY_H__

namespace dedupv1 {
namespace base {

/**
 * A pointer wrapper that is deleted as soon as the variable leaves the scope
 */
template<class T> class ScopedPtr {
    private:
        DISALLOW_COPY_AND_ASSIGN(ScopedPtr);

        /**
         * pointer
         */
        T* p_;
    public:
        /**
         * Constructor.
         *
         * @param p
         * @return
         */
        inline explicit ScopedPtr(T* p) {
            this->p_ = p;
        }

		/**
		 * destructor.
		 * If the pointer is set, the 
		 * object is released
		 */
        inline ~ScopedPtr() {
            if (p_ != NULL) {
                delete p_;
            }
        }

        inline T& operator*() {
            return *p_;
        }

        inline T* operator->()  {
            return p_;
        }

        inline T* Get() {
            return p_;
        }

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Set(T* p) {
            if(this->p_) {
                return false;
            }
            this->p_ = p;
            return true;
        }

        /**
         * Releases the pointer.
         * Use this if the pointer should not be deleted at the end of the scope or
         * if it is deleted manually. It is not possible and even might lead to an
         * segmentation fault to try to access the pointer using the scoped pointer after this call.
         */
        inline T* Release() {
            T* r = p_;
            this->p_ = NULL;
            return r;
        }
};

/**
 * Wrapper around an array that deletes the array as soon as the variable
 * leaves the scope
 */
template<class T> class ScopedArray {
    private:
        DISALLOW_COPY_AND_ASSIGN(ScopedArray);

        /**
         * pointer
         */
        T* p_;
    public:
        /**
         * Constructor.
         *
         * @param p
         * @return
         */
        inline explicit ScopedArray(T* p) {
            this->p_ = p;
        }

        ~ScopedArray() {
            if (p_ != NULL) {
                delete [] p_;
            }
        }

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Set(T* p) {
            if(p_) {
                return false;
            }
            this->p_ = p;
            return true;
        }

        inline T& operator*() {
            return *p_;
        }

        inline T* operator->()  {
            return p_;
        }

        inline T* Get() {
            return p_;
        }

        /**
         * Releases the array.
         * Use this if the array pointer should not be deleted at the end of the scope or
         * if it is deleted manually. It is not possible and even might lead to an
         * segmentation fault to try to access the array pointer using the scoped array after this call.
         */
        inline T* Release() {
            T* r = p_;
            this->p_ = NULL;
            return r;
        }
};

}
}

#endif  // MEMORY_H__
