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

#include <base/semaphore.h>
#include <base/logging.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>

LOGGER("Semaphore");

namespace dedupv1 {
namespace base {

Semaphore::Semaphore(int value) {
#ifdef __APPLE__
    {
        char sem_name[64];
        snprintf(sem_name, sizeof(sem_name), "dedupv1_sem_%p", this);
        this->sem_ = sem_open(sem_name, O_CREAT, 0777, value);
        if (this->sem_ == reinterpret_cast<sem_t*>(SEM_FAILED)) {
            ERROR("Cannot open semaphore: " << strerror(errno));
            this->sem_ = NULL;
        }
    }
#else
    this->sem_ = new sem_t;
    if (sem_init(this->sem_, 0, value) != 0) {
        WARNING("Semaphore init failed: " << strerror(errno));
        delete this->sem_;
        this->sem_ = NULL;
    }
#endif
}

bool Semaphore::Wait() {
    return sem_wait(this->sem_) == 0;
}

#ifdef __APPLE__
bool pthread_sleep(int s) {
    pthread_mutex_t mutex;
    pthread_cond_t conditionvar;

    CHECK(pthread_mutex_init(&mutex, NULL) == 0, "Cannot init mutex");
    CHECK(pthread_cond_init(&conditionvar, NULL) == 0, "Cannot init condition");

    struct timespec ts;
    ts.tv_sec = (unsigned int) time(NULL) + s;
    ts.tv_nsec = 0;

    int r = pthread_cond_timedwait(&conditionvar, &mutex, &ts);
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&conditionvar);

    return r == ETIMEDOUT;
}
#endif

bool Semaphore::TryWait(bool* locked) {
    CHECK(this->sem_, "Semaphore not set");
    int r = sem_trywait(this->sem_);
    if (r == 0) {
        *locked = true;
        return true;
    }
    if (errno == EAGAIN) {
        *locked = false;
        return true;
    }
    ERROR(strerror(errno));
    return false;
}

bool Semaphore::Post() {
    CHECK(this->sem_, "Semaphore not set");
    return sem_post(this->sem_) == 0;
}

Semaphore::~Semaphore() {
#ifdef __APPLE__
    if (this->sem_) {
        sem_close(this->sem_);
        char sem_name[64];
        snprintf(sem_name, sizeof(sem_name), "dedupv1_sem_%p", this);
        sem_unlink(sem_name);
        this->sem_ = NULL;
    }
#else
    if (this->sem_) {
        sem_destroy(this->sem_);
        delete this->sem_;
        this->sem_ = NULL;
    }
#endif
}

}
}
