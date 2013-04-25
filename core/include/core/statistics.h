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

#ifndef STATISTICS_H__
#define STATISTICS_H__

#include <core/dedup.h>
#include <base/option.h>
#include <base/index.h>
#include <google/protobuf/message.h>
#include <string>
#include <map>

namespace dedupv1 {

/**
 * Statistics should be persisted in regular intervals.
 * How the statistic data is stored is not regulated (usually a simple data base).
 *
 * Each class that has statistics, is responsible to serialze its statistic into a
 * protobuf message. Statistic message should be created in *_stats.proto files and
 * have a StatsData suffix.
 */
class PersistStatistics {
        DISALLOW_COPY_AND_ASSIGN(PersistStatistics);
    public:
        /**
         * Constructor
         * @return
         */
        PersistStatistics();

        /**
         * Destructor
         * @return
         */
        virtual ~PersistStatistics();

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Persist(const std::string& key, const google::protobuf::Message& message) = 0;

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Restore(const std::string& key, google::protobuf::Message* message) = 0;

        virtual dedupv1::base::Option<bool> Exists(const std::string& key) = 0;
};

class MemoryPersistentStatistics : public PersistStatistics {
    private:
        std::map<std::string, std::string> stats_;
    public:

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Persist(const std::string& key, const google::protobuf::Message& message);

        /**
         * @return true iff ok, otherwise an error has occurred
         */
        virtual bool Restore(const std::string& key, google::protobuf::Message* message);

        virtual dedupv1::base::Option<bool> Exists(const std::string& key);
};

class IndexPersistentStatistics : public PersistStatistics {
    private:
        dedupv1::base::PersistentIndex* index_;
        bool started_;
    public:
        IndexPersistentStatistics();
        virtual ~IndexPersistentStatistics();

        /**
         *
         * Available options:
         * - type
         *
     * @return true iff ok, otherwise an error has occurred
         */
        bool Start(const dedupv1::StartContext& start_context);

        /**
         *
         * Available options:
         * - type
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool SetOption(const std::string& option_name, const std::string& option);

        virtual bool Persist(const std::string& key, const google::protobuf::Message& message);

        virtual bool Restore(const std::string& key, google::protobuf::Message* message);

        virtual dedupv1::base::Option<bool> Exists(const std::string& key);

#ifdef DEDUPV1_CORE_TEST
        bool data_cleared_;

        void ClearData();
#endif
};

class StatisticProvider {
    public:
        virtual bool PersistStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        virtual bool RestoreStatistics(std::string prefix, dedupv1::PersistStatistics* ps);

        /**
         * Print statistics.
         * @return
         */
        virtual std::string PrintStatistics();

        virtual std::string PrintProfile();

        virtual std::string PrintLockStatistics();

        virtual std::string PrintTrace();

};

}

#endif // STATISTICS_H__
