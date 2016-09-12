/*
 Copyright 2016 Goldman Sachs.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */

package com.gs.fw.common.mithra.cache;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;


public class ReadWriteLock
{

    private static final int READER_BITS = 25;
    private static final int READER_MASK = (1 << READER_BITS) - 1;

    private static final int WRITER_MASK = 1 << READER_BITS;
    private static final int PREPARING_TO_GO_LOCAL = 1 << (READER_BITS+1);
    private static final int PREPARING_TO_WRITE_MASK = 1 << (READER_BITS+2);
    private static final int GLOBAL_MASK = 1 << (READER_BITS+4);

    private static final int lockPower = 3;

    private static final int lockCount = 1 << lockPower;

    private static final int LOCK_MASK = (1 << lockPower) - 1;

    private final GlobalSync globalSync = new GlobalSync();
    private final LocalSync[] localSyncs = new LocalSync[lockCount];

    public ReadWriteLock()
    {
        for(int i=0;i<lockCount;i++)
        {
            localSyncs[i] = new LocalSync();
        }
    }

    // this is a dummy method to prevent removal of the cache line buffers. it should never be called.
    public long getCacheLineConflict()
    {
        return globalSync.getCacheLineConflict() + localSyncs[0].getCacheLineConflict();
    }

    public void acquireReadLock()
    {
        int readerId = getLocalSyncIndex();
        if (localSyncs[readerId].tryAcquireSharedLocal()) return;
        if (!globalSync.tryFastAcquireShared())
        {
            slowAcquireReadLock(readerId);
        }
    }

    private void slowAcquireReadLock(int readerId)
    {
        globalSync.acquireShared(readerId);
        if (globalSync.mustReadLockLocally())
        {
            //we're now in prepare for local mode
            localSyncs[readerId].tryAcquireSharedDeGlobalize();
            localSyncs[(readerId + 1) & LOCK_MASK].tryDeglobalize(); // spread the deglobalization
            globalSync.releaseShared(PREPARING_TO_GO_LOCAL);
        }
    }

    public void acquireWriteLock()
    {
        globalSync.acquire(GLOBAL_MASK);
        if (globalSync.mustPrepareWriteLock())
        {
            prepareLocalWriteLocksAndLockGlobal();
        }
    }

    private void prepareLocalWriteLocksAndLockGlobal()
    {
        int remainingPosition = -1;
        for(int i=0;i<lockCount;i++)
        {
            if (!localSyncs[i].tryAcquire(0))
            {
                remainingPosition = i;
            }
        }
        while(remainingPosition >= 0)
        {
            localSyncs[remainingPosition].acquire(0);
            remainingPosition--;
        }
        globalSync.acquire(GLOBAL_MASK | PREPARING_TO_WRITE_MASK);
    }

    public void release()
    {
        if (!globalSync.releaseReadOrWrite())
        {
            localSyncs[getLocalSyncIndex()].releaseShared(0);
        }
    }

    private int getLocalSyncIndex()
    {
        return ((int) Thread.currentThread().getId()) & LOCK_MASK;
    }

    /**
     * upgrade a read lock to a write lock. If a lock is already a write lock, this method does nothing.
     *
     * @return true if waited
     *         true if a read lock was upgraded with the possibility of having given away other write locks.
     *         false if the existing lock was a write lock or the upgrade was immediate, that is, no other write locks
     *         were taken
     */
    public boolean upgradeToWriteLock()
    {
        if (globalSync.trySingleReaderUpgrade()) return false;
        slowUpgrade();
        return true;
    }

    private void slowUpgrade()
    {
        this.release();
        this.acquireWriteLock();
    }

    private static final class GlobalSync extends AbstractQueuedSynchronizer
    {
        private Thread readPreparationThread;
        private Thread writePreparationThread;
        private Thread singularReaderThread;
        private int localBias;
        private long buf1,buf2,buf3,buf4; // to prevent cache line conflicts on adjacent locks.

        public GlobalSync()
        {
            this.setState(GLOBAL_MASK);
            buf1 = (long)(Math.random()*1000);
            buf2 = (long)(Math.random()*1000);
            buf3 = (long)(Math.random()*1000);
            buf4 = (long)(Math.random()*1000);

        }

        public long getCacheLineConflict()
        {
            return buf1+buf2+buf3+buf4;
        }

        /*
        acquires is expected to be either GLOBAL_MASK, or GLOBAL_MASK | PREPARING_TO_WRITE
         */
        @Override
        public boolean tryAcquire(int acquires)
        {
            localBias = 0;
            return compareAndSetState(acquires, WRITER_MASK | GLOBAL_MASK) || trySlowAcquire(acquires);
        }

        private boolean trySlowAcquire(int acquires)
        {
            for (; ;)
            {
                int c = getState();
                if ((c & (READER_MASK | PREPARING_TO_GO_LOCAL | WRITER_MASK)) != 0) return false;
                if (acquires == (GLOBAL_MASK | PREPARING_TO_WRITE_MASK))
                {
                    // we've locked all the locals and we're about to lock the global
                    if (compareAndSetState(GLOBAL_MASK | PREPARING_TO_WRITE_MASK, GLOBAL_MASK | WRITER_MASK)) return true;
                }
                else
                {
                    // we're here without having prepared the locals. we can either lock immediately, or prepare to lock
                    if ((c & PREPARING_TO_WRITE_MASK) != 0) return false;
                    if (c == GLOBAL_MASK)
                    {
                        if (compareAndSetState(GLOBAL_MASK, GLOBAL_MASK | WRITER_MASK)) return true;
                    }
                    else
                    {
                        // c must be zero
                        if (compareAndSetState(0, GLOBAL_MASK | PREPARING_TO_WRITE_MASK))
                        {
                            writePreparationThread = Thread.currentThread();
                            return true;
                        }
                    }
                }
            }
        }

        public boolean mustReadLockLocally()
        {
            if (Thread.currentThread() == readPreparationThread)
            {
                readPreparationThread = null;
                return true;
            }
            return false;
        }

        public boolean mustPrepareWriteLock()
        {
            if (Thread.currentThread() == writePreparationThread)
            {
                writePreparationThread = null;
                return true;
            }
            return false;
        }

        public boolean tryFastAcquireShared()
        {
            localBias++;
            if (localBias < 100 && compareAndSetState(GLOBAL_MASK, GLOBAL_MASK | 1))
            {
                singularReaderThread = Thread.currentThread();
                return true;
            }
            return false;
        }

        @Override
        public int tryAcquireShared(int unused)
        {
            for (; ;)
            {
                int c = getState();
                if (c == GLOBAL_MASK || c == 0)
                {
                    if (localBias < 100)
                    {
                        if (compareAndSetState(c, c | 1))
                        {
                            singularReaderThread = Thread.currentThread();
                            return 1;
                        }
                    }
                    else
                    {
                        if (compareAndSetState(c, (c & ~GLOBAL_MASK) | PREPARING_TO_GO_LOCAL))
                        {
                            localBias = 0;
                            readPreparationThread = Thread.currentThread();
                            return 0;
                        }
                    }
                }
                else if ((c & (PREPARING_TO_WRITE_MASK | WRITER_MASK | PREPARING_TO_GO_LOCAL)) == 0)
                {
                    if (compareAndSetState(c, (c & ~GLOBAL_MASK) | PREPARING_TO_GO_LOCAL))
                    {
                        readPreparationThread = Thread.currentThread();
                        return 0;
                    }
                }
                else return -1;
            }
        }

        @Override
        protected final boolean tryRelease(int releases)
        {
            compareAndSetState(GLOBAL_MASK | WRITER_MASK, GLOBAL_MASK);
            return true;
        }

        @Override
        protected final boolean tryReleaseShared(int releases)
        {
            if (releases == 0)
            {
                singularReaderThread = null;
                return compareAndSetState(GLOBAL_MASK | 1, GLOBAL_MASK) || slowReleaseShared();
            }
            else
            {
                releasePrepareForLocal();
                return true;
            }
        }

        private void releasePrepareForLocal()
        {
            for (; ;)
            {
                int c = getState();
                if (compareAndSetState(c, (c & ~PREPARING_TO_GO_LOCAL)))
                    return;
            }
        }

        private boolean slowReleaseShared()
        {
            for (; ;)
            {
                int c = getState();
                if (compareAndSetState(c, (c & ~READER_MASK)))
                    return true;
            }
        }

        public boolean trySingleReaderUpgrade()
        {
            if ((getState() & WRITER_MASK) != 0) return true;
            if (singularReaderThread == Thread.currentThread() &&
                    this.compareAndSetState(GLOBAL_MASK | 1, GLOBAL_MASK | WRITER_MASK))
            {
                singularReaderThread = null;
                return true;
            }
            return false;
        }

        @Override
        public final boolean isHeldExclusively()
        {
            return (getState() & WRITER_MASK) != 0;
        }

        public boolean releaseReadOrWrite()
        {
            if (Thread.currentThread() == singularReaderThread)
            {
                this.releaseShared(0);
                return true;
            }
            if ((getState() & WRITER_MASK) != 0)
            {
                this.release(1);
                return true;
            }
            return false;
        }


    }

    private static final class LocalSync extends AbstractQueuedSynchronizer
    {
        private long buf1,buf2,buf3,buf4,buf5,buf6; // to prevent cache line conflicts on adjacent locks. Size found experimentally on Intel Xeon.

        private LocalSync()
        {
            setState(GLOBAL_MASK);
            buf1 = (long)(Math.random()*1000);
            buf2 = (long)(Math.random()*1000);
            buf3 = (long)(Math.random()*1000);
            buf4 = (long)(Math.random()*1000);
            buf5 = (long)(Math.random()*1000);
            buf6 = (long)(Math.random()*1000);
        }

        @Override
        public boolean tryAcquire(int acquires)
        {
            return compareAndSetState(0, GLOBAL_MASK) || compareAndSetState(GLOBAL_MASK, GLOBAL_MASK);
        }

        public boolean tryAcquireSharedLocal()
        {
            return compareAndSetState(0, 1) || tryAcquireSharedSlowLocal();
        }

        private boolean tryAcquireSharedSlowLocal()
        {
            for (; ;)
            {
                int c = getState();
                if ((c & GLOBAL_MASK) != 0)
                    return false;
                if (compareAndSetState(c, c + 1))
                    return true;
                // Recheck if lost CAS
            }
        }

        public void tryAcquireSharedDeGlobalize()
        {
            if (compareAndSetState(GLOBAL_MASK, 1)) return;
            tryAcquireSharedSlowDeGlobalize();
        }

        private void tryAcquireSharedSlowDeGlobalize()
        {
            for (; ;)
            {
                int c = getState();
                if (compareAndSetState(c, (c & ~GLOBAL_MASK) + 1))
                    return;
                // Recheck if lost CAS
            }
        }

        @Override
        protected final boolean tryReleaseShared(int releases)
        {
            return compareAndSetState(1, 0) || trySlowReleaseShared();
        }

        private boolean trySlowReleaseShared()
        {
            for (; ;)
            {
                int c = getState();
                if (compareAndSetState(c, (c & GLOBAL_MASK) | (c & ~GLOBAL_MASK) - 1))
                    return (c & ~GLOBAL_MASK) == 1;
            }
        }

        public void tryDeglobalize()
        {
            this.compareAndSetState(GLOBAL_MASK, 0);
        }

        public long getCacheLineConflict()
        {
            return buf1+buf2+buf3+buf4+buf5+buf6;
        }

    }

}
