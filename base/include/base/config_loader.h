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
#ifndef CONFIG_LOADER_H__
#define CONFIG_LOADER_H__

#include <string>

#include <base/base.h>
#include <base/callback.h>

namespace dedupv1 {
namespace base {

/**
 * Parse a configuration file and calls the client back for
 * each option name/option pair
 */
class ConfigLoader {
        DISALLOW_COPY_AND_ASSIGN(ConfigLoader);
    private:
		/**
		 * Callback for all option name/option pair
		 */
        Callback2<bool, const std::string&, const std::string&>* option_callback_;

		/**
		 * Callback for situations when an error occurred
		 */
        Callback1<void, const std::string&>* error_callback_;

        /**
         * Holds the configuration.
         */
        std::string config_;

        static void NullErrorHandler(const std::string& error_message);

    public:
	    /**
	     * Constructor.
	     * The owner of the config loader is responsible that the callbacks
	     * are valid over the lifetime of the config load.
	     *
	     * @param option_callback
	     * @param error_callback (can be NULL)
	     */
        ConfigLoader(Callback2<bool, const std::string&, const std::string&>* option_callback, Callback1<void, const std::string& >* error_callback = NULL);

        /**
         * Destructor
         * @return
         */
        ~ConfigLoader();

        /**
         * Processed a given configuration file
         * @param filename configuration file to process. 
         * @return true iff ok, otherwise an error has occurred
         */
        bool ProcessFile(const std::string& filename);

		/**
		 * TODO (dmeister): Public ???
         * @return true iff ok, otherwise an error has occurred
		 */
        bool ProcessLine(const std::string& configuration_line, uint32_t line_no);

        /**
         * returns the complete configuration data
         */
        inline const std::string& config_data() const;
};

const std::string& ConfigLoader::config_data() const {
    return config_;
}

}
}

#endif // CONFIG_LOADER_H__
