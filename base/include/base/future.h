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

#ifndef FUTURE_H__
#define FUTURE_H__

#include <base/base.h>
#include <base/option.h>
#include <base/logging.h>
#include <base/locks.h>

namespace dedupv1 {
namespace base {

namespace internal {
extern LOGGER_CLASS kFutureLogger;
}

/**
 *
 * Future is a design pattern described in "POSA II".
 *
 * This is a simple implementation of a Future. It used a
 * reference count approach to managed the shared usage.
 *
 * A future cannot be allocated on the stack.
 */
template<class RT> class FutureObject {
    private:
        DISALLOW_COPY_AND_ASSIGN(FutureObject);

    /**
     * Lock to project the future members.
     * This lock is also used to wait on the condition
     */
        MutexLock lock_;

    /**
     * current reference count
     */
        uint32_t ref_count_;

    /**
     * stores the value if the value is set
     */
        RT value_;

    /**
     * iff the value is set
     */
        bool value_set_;

    /**
     * iff the usage of the future is aborted.
     * The value will never be set in the future
     */
      bool abort_;

    /**
     * Condition clients of the future wait on
     */
        Condition condition_;
    public:
    /**
     * Constructor.
     * The reference count is set to 1. That means that the initial
     * referencer shall not call AddRef().
     */
        FutureObject() {
            ref_count_ = 1; // the creator is the first user
            value_set_ = false;
            abort_ = false;
        }

    /**
     * Adds a new reference
     * All clients that store a reference to the future must call this method.
     */
        bool AddRef() {
            if (!lock_.AcquireLock()) {
                ERROR_LOGGER(internal::kFutureLogger, "Failed to acquire future lock");
                return false;
            }
            ref_count_++;
            lock_.ReleaseLock();
            return true;
        }

    /**
     * returns the value of the future.
     * If the value is not set and if it is not aborted, the method
     * blocks
     * @return true iff ok, otherwise an error has occurred. If true, the value is set
     */
        bool Get(RT* v) {
            if (!v) {
                ERROR_LOGGER(internal::kFutureLogger, "Value not set");
                return false;
            }
            if (!lock_.AcquireLock()) {
                ERROR_LOGGER(internal::kFutureLogger, "Failed to acquire future lock");
                return false;
            }
            while (!value_set_ && !abort_) {
                if (!condition_.ConditionWait(&lock_)) {
                    ERROR_LOGGER(internal::kFutureLogger, "Failed to wait for future condition");
                    lock_.ReleaseLock();
                    return false;
                }
            }
            if (abort_) {
                lock_.ReleaseLock();
                return false;
            }
            *v = value_;
            lock_.ReleaseLock();
            return true;
        }

        /**
         * Waits until the future is aborted or until the value is set.
         * After the method returns, the abort flag or the value is set.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool Wait() {
            if (!lock_.AcquireLock()) {
                ERROR_LOGGER(internal::kFutureLogger, "Failed to acquire future lock");
                return false;
            }
            while (!value_set_ && !abort_) {
                if (!condition_.ConditionWait(&lock_)) {
                    ERROR_LOGGER(internal::kFutureLogger, "Failed to wait for future condition");
                    lock_.ReleaseLock();
                    return false;
                }
            }
            lock_.ReleaseLock();
            return true;
        }

        /**
         * Waits for the given future, but only for the given number of seconds (s).
         *
         * Returns an unset option (false) if an error occured. If the option is Some(false), the
         * waiting is aborted due to a timeout. If the result is Some(true), the future is finished and
         * the waiting succeeded. Now either the value is set or the future is aborted.
         */
        dedupv1::base::Option<bool> WaitTimeout(uint32_t s) {
            if (!lock_.AcquireLock()) {
                ERROR_LOGGER(internal::kFutureLogger, "Failed to acquire future lock");
                return false;
            }
            if (!value_set_ && !abort_) {
                enum timed_bool tb = condition_.ConditionWaitTimeout(&lock_, s);
                if (tb == TIMED_FALSE) {
                    ERROR_LOGGER(internal::kFutureLogger, "Failed to wait for future condition");
                    lock_.ReleaseLock();
                    return false;
                } else if (tb == TIMED_TIMEOUT) {
                    lock_.ReleaseLock();
                    return make_option(false);
                }
            }
            lock_.ReleaseLock();
            return make_option(true);
        }

    /**
     * returns true iff the future is aborted.
     */
        bool is_abort() {
            if (!lock_.AcquireLock()) {
                ERROR_LOGGER(internal::kFutureLogger, "Failed to acquire future lock");
                return false;
            }
            bool a = abort_;
            lock_.ReleaseLock();
            return a;
        }

        /**
         * returns true iff the value is set
         * @return
         */
        bool is_value_set() {
            if (!lock_.AcquireLock()) {
                ERROR_LOGGER(internal::kFutureLogger, "Failed to acquire future lock");
            }
            bool a = value_set_;
            lock_.ReleaseLock();
            return a;
        }

        /**
         * Sets the value.
         * @param value
         * @return true iff ok, otherwise an error has occurred
         */
        bool Set(RT value) {
            if (!lock_.AcquireLock()) {
                ERROR_LOGGER(internal::kFutureLogger, "Failed to acquire future lock");
                return false;
            }
            if (value_set_ || abort_) {
                lock_.ReleaseLock();
                return false;
            }
            value_ = value;
            value_set_ = true;
            if (!condition_.Broadcast()) {
                ERROR_LOGGER(internal::kFutureLogger, "Failed to broadcast future condition");
                lock_.ReleaseLock();
                return false;
            }
            lock_.ReleaseLock();
            return true;
        }

    /**
     * Aborts the future
     * @return true iff ok, otherwise an error has occurred
     */
        bool Abort() {
            if (!lock_.AcquireLock()) {
                ERROR_LOGGER(internal::kFutureLogger, "Failed to acquire future lock");
                return false;
            }
            if (value_set_ || abort_) {
                lock_.ReleaseLock();
                return false;
            }
            abort_ = true;
            if (!condition_.Broadcast()) {
                lock_.ReleaseLock();
                return false;
            }
            lock_.ReleaseLock();
            return true;
        }

        bool Release() {
            bool d = false;
            lock_.AcquireLock();
            ref_count_--;
            if (ref_count_ == 0) {
                d = true;
            }
            lock_.ReleaseLock();
            return d;
        }
};

template<class RT> class Future {
  private:
        DISALLOW_COPY_AND_ASSIGN(Future);
    FutureObject<RT>* instance_;

    explicit Future(FutureObject<RT>* instance) : instance_(instance) {
    }
  public:
    /**
     * Constructor.
     * The reference count is set to 1. That means that the initial
     * referencer shall not call AddRef().
     */
    Future() {
      instance_ = new FutureObject<RT>();
    }

    /**
     * Adds a new reference
     * All clients that store a reference to the future must call this method.
     */
        Future<RT>* AddRef() {
            if (!instance_->AddRef()) {
              return NULL;
            }
            return new Future<RT>(instance_);
        }

    /**
     * returns the value of the future.
     * If the value is not set and if it is not aborted, the method
     * blocks
     * @return true iff ok, otherwise an error has occurred. If true, the value is set
     */
        bool Get(RT* v) {
            return instance_->Get(v);
        }

        /**
         * Waits until the future is aborted or until the value is set.
         * After the method returns, the abort flag or the value is set.
         *
         * @return true iff ok, otherwise an error has occurred
         */
        bool Wait() {
            return instance_->Wait();
        }

        /**
         * Waits for the given future, but only for the given number of seconds (s).
         *
         * Returns an unset option (false) if an error occured. If the option is Some(false), the
         * waiting is aborted due to a timeout. If the result is Some(true), the future is finished and
         * the waiting succeeded. Now either the value is set or the future is aborted.
         */
        dedupv1::base::Option<bool> WaitTimeout(uint32_t s) {
            return instance_->WaitTimeout(s);
        }

    /**
     * returns true iff the future is aborted.
     */
        bool is_abort() {
            return instance_->is_abort();
        }

        /**
         * returns true iff the value is set
         * @return
         */
        bool is_value_set() {
            return instance_->is_value_set();
        }

        /**
         * Sets the value.
         * @param value
         * @return true iff ok, otherwise an error has occurred
         */
        bool Set(RT value) {
            return instance_->Set(value);
        }

    /**
     * Aborts the future
     * @return true iff ok, otherwise an error has occurred
     */
        bool Abort() {
            return instance_->Abort();
        }

    ~Future() {
      if (instance_->Release()) {
        delete instance_;
      }
    }
};


}
}


#endif /* FUTURE_H_ */
