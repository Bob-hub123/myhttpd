#ifndef MYHTTPD_LOCKER_H
#define MYHTTPD_LOCKER_H

#include <cerrno>
#include <pthread.h>
#include <semaphore.h>

class MutexLock {
public:
    MutexLock()
    {
        pthread_mutex_init(&m_mutex, nullptr);
    }

    ~MutexLock()
    {
        pthread_mutex_destroy(&m_mutex);
    }

    void lock()
    {
        pthread_mutex_lock(&m_mutex);
    }

    void unlock()
    {
        pthread_mutex_unlock(&m_mutex);
    }

    pthread_mutex_t* get()
    {
        return &m_mutex;
    }

    MutexLock(const MutexLock&) = delete;
    MutexLock& operator=(const MutexLock&) = delete;

private:
    pthread_mutex_t m_mutex;
};

class MutexLockGuard {
public:
    explicit MutexLockGuard(MutexLock& mutex)
        : m_mutex(mutex)
    {
        m_mutex.lock();
    }

    ~MutexLockGuard()
    {
        m_mutex.unlock();
    }

    MutexLockGuard(const MutexLockGuard&) = delete;
    MutexLockGuard& operator=(const MutexLockGuard&) = delete;

private:
    MutexLock& m_mutex;
};

class Sem {
public:
    explicit Sem(int value)
    {
        sem_init(&m_sem, 0, static_cast<unsigned int>(value));
    }

    ~Sem()
    {
        sem_destroy(&m_sem);
    }

    bool wait()
    {
        while (sem_wait(&m_sem) == -1) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        return true;
    }

    bool post()
    {
        return sem_post(&m_sem) == 0;
    }

    Sem(const Sem&) = delete;
    Sem& operator=(const Sem&) = delete;

private:
    sem_t m_sem;
};

#endif
