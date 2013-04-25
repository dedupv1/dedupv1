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

#ifndef INFO_STORE_H_
#define INFO_STORE_H_

#include <core/dedup.h>
#include <base/option.h>
#include <base/index.h>
#include <google/protobuf/message.h>
#include <string>
#include <map>

namespace dedupv1 {

/**
 * The info store is used to store persistent state information about
 * different components of the dedupv1 system.
 */
class InfoStore {
        DISALLOW_COPY_AND_ASSIGN(InfoStore);
    public:
        /**
         * Constructor
         * @return
         */
        InfoStore();

        /**
         * Destructor
         * @return
         */
        virtual ~InfoStore();

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Start(const dedupv1::StartContext& start_context);

        /**
         *
         * No svailable options
         *
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool PersistInfo(std::string key, const google::protobuf::Message& message) = 0;

        virtual dedupv1::base::lookup_result RestoreInfo(std::string key, google::protobuf::Message* message) = 0;

#ifdef DEDUPV1_CORE_TEST
        virtual void ClearData();
#endif
};

class MemoryInfoStore : public InfoStore {
    private:
        std::map<std::string, std::string> stats_;
    public:
        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool PersistInfo(std::string key, const google::protobuf::Message& message);

        virtual dedupv1::base::lookup_result RestoreInfo(std::string key, google::protobuf::Message* message);
};

class IndexInfoStore : public InfoStore {
    private:
        dedupv1::base::PersistentIndex* index_;
        bool started_;
    public:
        IndexInfoStore();
        virtual ~IndexInfoStore();

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool PersistInfo(std::string key, const google::protobuf::Message& message);

        virtual dedupv1::base::lookup_result RestoreInfo(std::string key, google::protobuf::Message* message);

#ifdef DEDUPV1_CORE_TEST
        bool data_cleared_;

        void ClearData();
#endif
};

}

#endif /* INFO_STORE_H_ */
