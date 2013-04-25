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

#ifndef FACTORY_H_
#define FACTORY_H_

#include <string>
#include <map>

#include <base/base.h>
#include <base/logging.h>

/**
 * A factory is used as a template for different class factories.
 *
 * Different types can be registered via factory methods at a meta
 * factory. If a new instance of a given type (via string) should
 * be created, the factory method is called.
 *
 * @sa http://en.wikipedia.org/wiki/Factory_method_pattern
 */
template<class T> class MetaFactory {
        DISALLOW_COPY_AND_ASSIGN(MetaFactory);
    public:
        /**
         * Constructor
         * @return
         */
        MetaFactory(const std::string& logger_name, const std::string& type_name) :
            type_name_(type_name),
            logger_(GET_LOGGER(logger_name)) {
        }

        /**
         * Registers a new index type
         * @param name
         * @param factory
         * @return true iff ok, otherwise an error has occurred
         */
        bool Register(const std::string& name, T*(*factory)(void)) {
            if (factory == NULL) {
                ERROR_LOGGER(logger_, "Factory method not set");
                return false;
            }
            if (name.size() == 0) {
                ERROR_LOGGER(logger_, "Name not set");
                return false;
            }
            typename FactoryType::iterator i = factory_map_.find(name);
            if (i != factory_map_.end()) {
                ERROR_LOGGER(logger_, "" << type_name_ << " type already specified: name " << name);
                return false;
            }
            factory_map_[name] = factory;
            return true;
        }

        /**
         * Creates a new index of a given type
         * @param name
         * @return
         */
        T* Create(const std::string& name) {
            typename FactoryType::iterator i = factory_map_.find(name);
            if (i != factory_map_.end()) {
                T* (*f)(void) = i->second;
                T* o = f();
                if (!o) {
                    ERROR_LOGGER(logger_, "Cannot create new " << type_name_ << ": " << name);
                    return NULL;
                }
                return o;
            }
            std::string available_types;
            for (i = factory_map_.begin(); i != factory_map_.end(); i++) {
                if (i != factory_map_.begin()) {
                    available_types += ", ";
                }
                available_types += i->first;
            }
            ERROR_LOGGER(logger_, "Cannot find " << type_name_ << ": " << name << ", available types " << available_types);
            return NULL;
        }

    private:
        typedef T*(*FactoryPointerType)(void);
        typedef std::map<std::string, FactoryPointerType> FactoryType;
        std::string type_name_;

        LOGGER_CLASS logger_;

        /**
         * map that holds a mapping from the type name
         * to the creation function pointer
         */
        std::map<std::string, T*(*)(void)> factory_map_;
};

#endif /* FACTORY_H_ */
