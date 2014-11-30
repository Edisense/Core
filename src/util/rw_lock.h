#ifndef RW_LOCK_H
#define RW_LOCK_H

#include <pthread.h>
#include <cassert>

class RWLock
{
public:
	RWLock()
	{
		if(pthread_rwlock_init(&rw_lock, NULL) != 0)
		{
			throw "failed to init rw lock";
		}
	}
	~RWLock()
	{
		pthread_rwlock_destroy(&rw_lock);
	}
	void acquireRDLock()
	{
		assert(pthread_rwlock_rdlock(&rw_lock) == 0);
	}
	void releaseRDLock()
	{
		assert(pthread_rwlock_unlock(&rw_lock) == 0);
	}
	void acquireWRLock()
	{
		assert(pthread_rwlock_wrlock(&rw_lock) == 0);
	}
	void releaseWRLock()
	{
		assert(pthread_rwlock_unlock(&rw_lock) == 0);
	}
private:
	pthread_rwlock_t rw_lock;
};

#endif /* RW_LOCK_H */