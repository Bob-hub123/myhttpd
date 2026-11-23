#ifndef MYHTTPD_THREADPOOL_H
#define MYHTTPD_THREADPOOL_H

#include <cerrno>
#include <cstring>
#include <functional>
#include <iostream>
#include <list>
#include <stdexcept>
#include <string>

#include "locker.h"

template <typename T>
class ThreadPool {
public:
    ThreadPool(int threadNumber, int maxRequests)
        : m_threadNumber(threadNumber),
          m_maxRequests(maxRequests),
          m_threads(nullptr),
          m_sem(0),
          m_stop(false)
    {
        if (m_threadNumber <= 0) {
            throw std::invalid_argument("threadNumber must be positive");
        }
        if (m_maxRequests <= 0) {
            throw std::invalid_argument("maxRequests must be positive");
        }

        m_threads = new pthread_t[m_threadNumber];
        for (int i = 0; i < m_threadNumber; ++i) {
            const int ret = pthread_create(&m_threads[i], nullptr, worker, this);
            if (ret != 0) {
                stopCreatedThreads(i);
                throw std::runtime_error(
                    "pthread_create failed: " + std::string(std::strerror(ret)));
            }
        }
    }

    ~ThreadPool()
    {
        {
            MutexLockGuard lock(m_mutex);
            m_stop = true;
        }

        for (int i = 0; i < m_threadNumber; ++i) {
            m_sem.post();
        }
        for (int i = 0; i < m_threadNumber; ++i) {
            pthread_join(m_threads[i], nullptr);
        }

        delete[] m_threads;
    }

    bool append(const T& task)
    {
        MutexLockGuard lock(m_mutex);
        if (static_cast<int>(m_workQueue.size()) >= m_maxRequests) {
            return false;
        }
        m_workQueue.push_back(task);
        m_sem.post();
        return true;
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

private:
    static void* worker(void* arg)
    {
        ThreadPool* pool = static_cast<ThreadPool*>(arg);
        pool->run();
        return nullptr;
    }

    void run()
    {
        while (true) {
            if (!m_sem.wait()) {
                continue;
            }

            T task;
            {
                MutexLockGuard lock(m_mutex);
                if (m_stop && m_workQueue.empty()) {
                    break;
                }
                if (m_workQueue.empty()) {
                    continue;
                }
                task = m_workQueue.front();
                m_workQueue.pop_front();
            }
            task();
        }
    }

    void stopCreatedThreads(int created)
    {
        {
            MutexLockGuard lock(m_mutex);
            m_stop = true;
        }
        for (int i = 0; i < created; ++i) {
            m_sem.post();
        }
        for (int i = 0; i < created; ++i) {
            pthread_join(m_threads[i], nullptr);
        }

        delete[] m_threads;
        m_threads = nullptr;
        m_threadNumber = 0;
    }

    int m_threadNumber;
    int m_maxRequests;
    pthread_t* m_threads;
    std::list<T> m_workQueue;
    MutexLock m_mutex;
    Sem m_sem;
    bool m_stop;
};

#endif
