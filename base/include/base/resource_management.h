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
#ifndef RESOURCE_MANAGEMENT_H__
#define RESOURCE_MANAGEMENT_H__

#include <base/base.h>
#include <base/locks.h>

#include <apr-1/apr.h>
#include <apr-1/apu.h>
#include <apr-1/apr_pools.h>
#include <apr-1/apr_errno.h>
#include <apr-1/apr_time.h>
#include <apr-1/apr_reslist.h>

#include <string>
#include <sstream>

namespace dedupv1 {
namespace base {
namespace internal {

/**
 * internal implementation class for the
 * resource management.
 * The implementation is backed by the
 * apr-util library.
 */
class ResourceManagementImpl {
    private:
        DISALLOW_COPY_AND_ASSIGN(ResourceManagementImpl);

        /**
         * pointer to an apr pool
         */
        apr_pool_t* pool;

        /**
         * pointer to an apr resource list
         */
        apr_reslist_t* resource_list;

        /**
         * name of the resource management pool
         */
        std::string name;

        /**
         * maximal number of resources that are allowed
         * in this pool.
         * If negative, an unlimited number is possible
         */
        int max;

        bool enforce_max_size_;
    public:
        /**
         * Constructor
         * @return
         */
        ResourceManagementImpl();

        /**
         * Destructor
         * @return
         */
        ~ResourceManagementImpl();

        /**
         * Acquires a new resource
         */
        void* Acquire();

        /**
         * Releases a resource and makes it available.
         *
         * @param resource
         * @return true iff ok, otherwise an error has occurred
         */
        bool Release(void* resource);

        /**
         * Init the resource management
         *
         * @param name
         * @param con
         * @param de
         * @param rm
         * @param maximal_size
         * @return true iff ok, otherwise an error has occurred
         */
        bool Init(const std::string& name,
                apr_reslist_constructor con,
                apr_reslist_destructor de,
                void* rm,
                int maximal_size,
                bool enforce_max_size);

        /**
         * returns the number of currently acquired, but not yet
         * released resources.
         */
        int GetAcquiredCount();
};

} // end of internal namespace

/**
 * Resource type interface that can be
 * implemented so that a new type of resources
 * can be handled by the resource management
 */
template<class T> class ResourceType {
    private:
        DISALLOW_COPY_AND_ASSIGN(ResourceType);
    public:
        /**
         * Constructor
         * @return
         */
        ResourceType() {
        }

        /**
         * Destructor
         * @return
         */
        virtual ~ResourceType() {
        }

        /**
         * creation method that should return a newly
         * created resource.
         * @return
         */
        virtual T* Create() = 0;

        /**
         * Optional method to reinitialize a given resource.
         *
         * @param resource
         */
        virtual void Reinit(T* resource) {
        }

        /**
         * Closes the resource and frees the resources.
         * This does not close the resource type itself.
         *
         * @param resource
         */
        virtual void Close(T* resource) = 0;
};

/**
 * The resource management is used for heavy objects that
 * are very expensive to create. The resource management
 * buffers a variable number of pooled objects that can be
 * acquired and released by client code.
 *
 * The strategy how the objects are created, destroyed is given
 * by an instance of the ResourceType.
 *
 * To avoid memory leaks it is important that every acquired
 * object is released eventually. The helper class
 * ScopedRelease can be used to easy that.
 *
 * The resource management pool is thread-safe.
 */
template<class T> class ResourceManagement {
        DISALLOW_COPY_AND_ASSIGN(ResourceManagement);

        /**
         * Pointer to the internal implementation of the
         * resource management
         */
        dedupv1::base::internal::ResourceManagementImpl* impl;

        /**
         * Pointer to the resource type.
         * Note that the resource management has the ownership
         * of the resource type.
         */
        ResourceType<T>* type;

        /**
         * implementation detail that is used as apr pool contructor.
         *
         * @param resource
         * @param params
         * @param pool
         * @return
         */
        static apr_status_t Ctor(void** resource, void* params, apr_pool_t* pool);

        /**
         * implementation detail that is used as apr pool destructor.
         *
         * @param resource
         * @param params
         * @param pool
         * @return
         */
        static apr_status_t Dtor(void* resource, void* params, apr_pool_t* pool);
    public:
        /**
         * Constructor.
         * @return
         */
        ResourceManagement();

        ~ResourceManagement();

        /**
         * @param name name of the resource to manage
         * @param maximal_size maximal number of resources that should be created.
         * @param type type of the resource. Note: resource management takes the ownership.
         */
        bool Init(const std::string& name, int maximal_size, ResourceType<T>* type, bool enforce_max_size = true);

        /**
         * Acquires a new resource of the given type.
         * The turned resource pointer is either newly created
         * or reinitized. After the usage the
         * resource pointer should be released to the pool.
         *
         * @return
         */
        inline T* Acquire();

        /**
         * Releases the resource pointer and makes it
         * available for other uses.
         *
         * @param resource
         * @return
         */
        inline bool Release(T* resource);

        /**
         * returns the number of acquired resources.
         */
        inline int GetAcquiredCount() {
            if (!impl) {
                return 0;
            }
            return impl->GetAcquiredCount();
        }
};

template<class T> class ScopedRelease {
        T* p;
        ResourceManagement<T>* rm;
        DISALLOW_COPY_AND_ASSIGN(ScopedRelease);

    public:
        inline explicit ScopedRelease(ResourceManagement<T>* rm) {
            this->rm = rm;
            this->p = NULL;
        }

        inline bool Acquire() {
            if (this->rm == NULL) return false;
            this->p = this->rm->Acquire();
            return (this->p != NULL);
        }

        inline bool Release() {
            if (this->p) {
                if (!this->rm->Release(this->p)) {
                    return false;
                }
                this->p = NULL;
            }
            return true;
        }

        inline ~ScopedRelease() {
            if (rm) {
                if (p != NULL) {
                    this->rm->Release(p);
                    p = NULL;
                }
            }
        }

        inline T& operator*() {
            return *p;
        }

        inline T* operator->()  {
            return p;
        }

        inline T* Get() {
            return p;
        }
};

template<class T> ResourceManagement<T>::ResourceManagement() {
        type = NULL;
        impl = new dedupv1::base::internal::ResourceManagementImpl();
}

template<class T> bool ResourceManagement<T>::Init(const std::string& name, int maximal_size, ResourceType<T>* type, bool enforce_max_size) {
        this->type = type;
        return this->impl->Init(name, &ResourceManagement::Ctor, &ResourceManagement::Dtor, this, maximal_size, enforce_max_size);
}

template<class T> T* ResourceManagement<T>::Acquire() {
        T* resource = static_cast<T*>(this->impl->Acquire());
        if (resource) {
            this->type->Reinit(resource);
        }
        return resource;
}

template<class T> bool ResourceManagement<T>::Release(T* resource) {
        return this->impl->Release(resource);
}

template<class T> ResourceManagement<T>::~ResourceManagement() {
        if (this->impl) {
            delete this->impl;
            this->impl = NULL;
        }
        if (this->type) {
            delete type;
            this->type = NULL;
        }
}

template<class T> apr_status_t ResourceManagement<T>::Ctor(void** resource, void* params, apr_pool_t* pool) {
        ResourceManagement<T>* rm = reinterpret_cast<ResourceManagement<T>*>(params);
        ResourceType<T>* t = rm->type;

        void* r = t->Create();
        if (!r) {
            resource = NULL;
            return APR_EINIT;
        }
        *resource = r;
        return 0;
}

template<class T> apr_status_t ResourceManagement<T>::Dtor(void* resource, void* params, apr_pool_t* pool) {
        ResourceManagement<T>* rm = reinterpret_cast<ResourceManagement<T>*>(params);
        ResourceType<T>* t = rm->type;

        t->Close(reinterpret_cast<T*>(resource));
        return 0;
}

}
}

#endif  // RESOURCE_MANAGEMENT_H__
