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

package com.gs.fw.common.mithra.connectionmanager;


import com.gs.fw.common.mithra.util.DoUntilProcedure;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <p>
 * When coupled with the appropriate {@link PoolableObjectFactory},
 * <tt>GenericObjectPool</tt> provides robust pooling functionality for
 * arbitrary objects.
 * <p>
 * A <tt>GenericObjectPool</tt> provides a number of configurable parameters:
 * <ul>
 *  <li>
 *    {<i>maxActive</i>} controls the maximum number of objects that can
 *    be borrowed from the pool at one time.  When non-positive, there
 *    is no limit to the number of objects that may be active at one time.
 *    When {<i>maxActive</i>} is exceeded, the pool is said to be exhausted.
 *  </li>
 *  <li>
 *    {<i>maxIdle</i>} controls the maximum number of objects that can
 *    sit idle in the pool at any time.  When negative, there
 *    is no limit to the number of objects that may be idle at one time.
 *  </li>
 *  <li>
 *    {<i>whenExhaustedAction</i>} specifies the
 *    behaviour of the {@link #borrowObject} method when the pool is exhausted:
 *    <ul>
 *    <li>
 *      When {<i>whenExhaustedAction</i>} is
 *      {@link #WHEN_EXHAUSTED_FAIL}, {@link #borrowObject} will throw
 *      a {@link NoSuchElementException}
 *    </li>
 *    <li>
 *      When {<i>whenExhaustedAction</i>} is
 *      {@link #WHEN_EXHAUSTED_GROW}, {@link #borrowObject} will create a new
 *      object and return it(essentially making {<i>maxActive</i>}
 *      meaningless.)
 *    </li>
 *    <li>
 *      When {<i>whenExhaustedAction</i>}
 *      is {@link #WHEN_EXHAUSTED_BLOCK}, {@link #borrowObject} will block
 *      (invoke {@link Object#wait} until a new or idle object is available.
 *      If a positive {<i>maxWait</i>}
 *      value is supplied, the {@link #borrowObject} will block for at
 *      most that many milliseconds, after which a {@link NoSuchElementException}
 *      will be thrown.  If {<i>maxWait</i>} is non-positive,
 *      the {@link #borrowObject} method will block indefinitely.
 *    </li>
 *    </ul>
 *  </li>
 *  <li>
 *    When {<i>testOnBorrow</i>} is set, the pool will
 *    attempt to validate each object before it is returned from the
 *    {@link #borrowObject} method. (Using the provided factory's
 *    {@link PoolableObjectFactory#validateObject} method.)  Objects that fail
 *    to validate will be dropped from the pool, and a different object will
 *    be borrowed.
 *  </li>
 *  <li>
 *    When {<i>testOnReturn</i>} is set, the pool will
 *    attempt to validate each object before it is returned to the pool in the
 *    {@link #returnObject} method. (Using the provided factory's
 *    {@link PoolableObjectFactory#validateObject}
 *    method.)  Objects that fail to validate will be dropped from the pool.
 *  </li>
 * </ul>
 * <p>
 * Optionally, one may configure the pool to examine and possibly evict objects as they
 * sit idle in the pool.  This is performed by an "idle object eviction" thread, which
 * runs asynchronously.  The idle object eviction thread may be configured using the
 * following attributes:
 * <ul>
 *  <li>
 *   {<i>timeBetweenEvictionRunsMillis</i>}
 *   indicates how long the eviction thread should sleep before "runs" of examining
 *   idle objects.  When non-positive, no eviction thread will be launched.
 *  </li>
 *  <li>
 *   {<i>minEvictableIdleTimeMillis</i>}
 *   specifies the minimum amount of time that an object may sit idle in the pool
 *   before it is eligible for eviction due to idle time.  When non-positive, no object
 *   will be dropped from the pool due to idle time alone.
 *  </li>
 *  <li>
 *   {<i>softMinEvictableIdleTimeMillis</i>}
 *   specifies the minimum amount of time an object may sit idle in the pool
 *   before it is eligible for eviction by the idle object evictor
 *   (if any), with the extra condition that at least "minIdle" amount of object
 *   remain in the pool.  When non-positive, no objects will be evicted from the pool
 *   due to idle time alone.
 *  </li>
 * </ul>
 * <p>
 *
 * @author Rodney Waldhoff
 * @author Dirk Verbeeck
 * @author Sandy McArthur
 * @author Mohammad Rezaei - modified for Thread Affinity

 *
 */
public class ObjectPoolWithThreadAffinity<E>
{
    private static final Logger logger = LoggerFactory.getLogger(ObjectPoolWithThreadAffinity.class.getName());
    //--- public constants -------------------------------------------

    /**
     * A "when exhausted action" type indicating that when the pool is
     * exhausted (i.e., the maximum number of active objects has
     * been reached), the {@link #borrowObject}
     * method should fail, throwing a {@link NoSuchElementException}.
     *
     * @see #WHEN_EXHAUSTED_BLOCK
     * @see #WHEN_EXHAUSTED_GROW
     */
    public static final byte WHEN_EXHAUSTED_FAIL = 0;

    /**
     * A "when exhausted action" type indicating that when the pool
     * is exhausted (i.e., the maximum number
     * of active objects has been reached), the {@link #borrowObject}
     * method should block until a new object is available, or the
     * {@link #getMaxWait maximum wait time} has been reached.
     *
     * @see #WHEN_EXHAUSTED_FAIL
     * @see #WHEN_EXHAUSTED_GROW
     * @see #getMaxWait
     */
    public static final byte WHEN_EXHAUSTED_BLOCK = 1;

    /**
     * A "when exhausted action" type indicating that when the pool is
     * exhausted (i.e., the maximum number
     * of active objects has been reached), the {@link #borrowObject}
     * method should simply create a new object anyway.
     *
     * @see #WHEN_EXHAUSTED_FAIL
     * @see #WHEN_EXHAUSTED_GROW
     */
    public static final byte WHEN_EXHAUSTED_GROW = 2;

    /**
     * The default cap on the number of "sleeping" instances in the pool.
     *
     * @see #getMaxIdle
     */
    public static final int DEFAULT_MAX_IDLE = 8;

    /**
     * The default minimum number of "sleeping" instances in the pool
     * before before the evictor thread (if active) spawns new objects.
     *
     * @see #getMinIdle
     */
    public static final int DEFAULT_MIN_IDLE = 0;

    /**
     * The default cap on the total number of active instances from the pool.
     *
     * @see #getMaxActive
     */
    public static final int DEFAULT_MAX_ACTIVE = 8;

    /**
     * The default "when exhausted action" for the pool.
     *
     * @see #WHEN_EXHAUSTED_BLOCK
     * @see #WHEN_EXHAUSTED_FAIL
     * @see #WHEN_EXHAUSTED_GROW
     */
    public static final byte DEFAULT_WHEN_EXHAUSTED_ACTION = WHEN_EXHAUSTED_BLOCK;

    /**
     * The default maximum amount of time (in millis) the
     * {@link #borrowObject} method should block before throwing
     * an exception when the pool is exhausted and the
     * {@link #getWhenExhaustedAction "when exhausted" action} is
     * {@link #WHEN_EXHAUSTED_BLOCK}.
     *
     * @see #getMaxWait
     */
    public static final long DEFAULT_MAX_WAIT = -1L;

    /**
     * The default "test on borrow" value.
     *
     * @see #getTestOnBorrow
     */
    public static final boolean DEFAULT_TEST_ON_BORROW = true;

    /**
     * The default "time between eviction runs" value.
     *
     * @see #getTimeBetweenEvictionRunsMillis
     */
    public static final long DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS = -1L;

    /**
     * The default value for {@link #getMinEvictableIdleTimeMillis}.
     *
     * @see #getMinEvictableIdleTimeMillis
     */
    public static final long DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 1000L * 60L * 30L;

    /**
     * The default value for {@link #getSoftMinEvictableIdleTimeMillis}.
     *
     * @see #getSoftMinEvictableIdleTimeMillis
     */
    public static final long DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS = 1000L * 60L * 5L;

    //--- package constants -------------------------------------------

    /**
     * Idle object evition Timer. Shared between all {@link ObjectPoolWithThreadAffinity}s
     */
    private static final Timer EVICTION_TIMER = new Timer("MithraConnectionPool Evictor", true);
    //--- private attributes ---------------------------------------

    /**
     * The cap on the number of idle instances in the pool.
     *
     * @see #getMaxIdle
     */
    private int maxIdle = DEFAULT_MAX_IDLE;

    /**
     * The cap on the minimum number of idle instances in the pool.
     *
     * @see #getMinIdle
     */
    private int minIdle = DEFAULT_MIN_IDLE;

    /**
     * The cap on the total number of active instances from the pool.
     *
     * @see #getMaxActive
     */
    private int maxActive = DEFAULT_MAX_ACTIVE;

    /**
     * The maximum amount of time (in millis) the
     * {@link #borrowObject} method should block before throwing
     * an exception when the pool is exhausted and the
     * {@link #getWhenExhaustedAction "when exhausted" action} is
     * {@link #WHEN_EXHAUSTED_BLOCK}.
     * <p/>
     * When less than or equal to 0, the {@link #borrowObject} method
     * may block indefinitely.
     *
     * @see #getMaxWait
     * @see #WHEN_EXHAUSTED_BLOCK
     * @see #getWhenExhaustedAction
     */
    private long maxWait = DEFAULT_MAX_WAIT;

    /**
     * The action to take when the {@link #borrowObject} method
     * is invoked when the pool is exhausted (the maximum number
     * of "active" objects has been reached).
     *
     * @see #WHEN_EXHAUSTED_BLOCK
     * @see #WHEN_EXHAUSTED_FAIL
     * @see #WHEN_EXHAUSTED_GROW
     * @see #DEFAULT_WHEN_EXHAUSTED_ACTION
     * @see #getWhenExhaustedAction
     */
    private byte whenExhaustedAction = DEFAULT_WHEN_EXHAUSTED_ACTION;

    /**
     * When <tt>true</tt>, objects will be
     * {@link PoolableObjectFactory#validateObject validated}
     * before being returned by the {@link #borrowObject}
     * method.  If the object fails to validate,
     * it will be dropped from the pool, and we will attempt
     * to borrow another.
     *
     * @see #getTestOnBorrow
     */
    private boolean testOnBorrow = DEFAULT_TEST_ON_BORROW;

    /**
     * The number of milliseconds to sleep between runs of the
     * idle object evictor thread.
     * When non-positive, no idle object evictor thread will be
     * run.
     *
     * @see #getTimeBetweenEvictionRunsMillis
     */
    private long timeBetweenEvictionRunsMillis = DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;

    /**
     * The minimum amount of time an object may sit idle in the pool
     * before it is eligible for eviction by the idle object evictor
     * (if any).
     * When non-positive, no objects will be evicted from the pool
     * due to idle time alone.
     *
     * @see #getMinEvictableIdleTimeMillis
     * @see #getTimeBetweenEvictionRunsMillis
     */
    private long minEvictableIdleTimeMillis = DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;

    /**
     * The minimum amount of time an object may sit idle in the pool
     * before it is eligible for eviction by the idle object evictor
     * (if any), with the extra condition that at least
     * "minIdle" amount of object remain in the pool.
     * When non-positive, no objects will be evicted from the pool
     * due to idle time alone.
     *
     * @see #getSoftMinEvictableIdleTimeMillis
     */
    private long softMinEvictableIdleTimeMillis = DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS;

    private LruListWithThreadAffinity<E> pool = null;

    private final PoolableObjectFactory<E> factory;

    /**
     * The number of objects {@link #borrowObject} borrowed
     * from the pool, but not yet returned.
     */
    private int numActive = 0;

    /**
     * My idle object eviction {@link TimerTask}, if any.
     */
    private Evictor evictor = null;

    private volatile boolean closed = false;

    //--- constructors -----------------------------------------------

    /**
     * Create a new <tt>GenericObjectPool</tt> using the specified values.
     *
     * @param factory                        the (possibly <tt>null</tt>)PoolableObjectFactory to use to create, validate and destroy objects
     * @param maxActive                      the maximum number of objects that can be borrowed from me at one time 
     * @param maxWait                        the maximum amount of time to wait for an idle object when the pool is exhausted an and <i>whenExhaustedAction</i> is {@link #WHEN_EXHAUSTED_BLOCK} (otherwise ignored) 
     * @param maxIdle                        the maximum number of idle objects in my pool 
     * @param minIdle                        the minimum number of idle objects in my pool 
     * @param testOnBorrow                   whether or not to validate objects before they are returned by the {@link #borrowObject} method
     * @param testOnReturn                   whether or not to validate objects after they are returned to the {@link #returnObject} method
     * @param timeBetweenEvictionRunsMillis  the amount of time (in milliseconds) to sleep between examining idle objects for eviction
     * @param minEvictableIdleTimeMillis     the minimum number of milliseconds an object can sit idle in the pool before it is eligible for eviction
     * @param softMinEvictableIdleTimeMillis the minimum number of milliseconds an object can sit idle in the pool before it is eligible for eviction with the extra condition that at least "minIdle" amount of object remain in the pool.
     * @since Pool 1.3
     */
    public ObjectPoolWithThreadAffinity(PoolableObjectFactory factory, int maxActive, long maxWait,
            int maxIdle, int minIdle, boolean testOnBorrow, boolean testOnReturn, long timeBetweenEvictionRunsMillis,
            long minEvictableIdleTimeMillis, long softMinEvictableIdleTimeMillis)
    {
        this.factory = factory;
        this.maxActive = maxActive;
        this.whenExhaustedAction = WHEN_EXHAUSTED_BLOCK;
        this.maxWait = maxWait;
        this.maxIdle = maxIdle;
        this.minIdle = minIdle;
        this.testOnBorrow = testOnBorrow;
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
        this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;

        pool = new LruListWithThreadAffinity();
        startEvictor(this.timeBetweenEvictionRunsMillis);
    }

    //--- public methods ---------------------------------------------

    //--- configuration methods --------------------------------------

    /**
     * Returns the cap on the total number of active instances from my pool.
     *
     * @return the cap on the total number of active instances from my pool.
     */
    public synchronized int getMaxActive()
    {
        return maxActive;
    }

    protected final boolean isClosed()
    {
        return closed;
    }

    /**
     * Returns the action to take when the {@link #borrowObject} method
     * is invoked when the pool is exhausted (the maximum number
     * of "active" objects has been reached).
     *
     * @return one of {@link #WHEN_EXHAUSTED_BLOCK}, {@link #WHEN_EXHAUSTED_FAIL} or {@link #WHEN_EXHAUSTED_GROW}
     */
    public synchronized byte getWhenExhaustedAction()
    {
        return whenExhaustedAction;
    }

    /**
     * Returns the maximum amount of time (in milliseconds) the
     * {@link #borrowObject} method should block before throwing
     * an exception when the pool is exhausted and the
     * {"when exhausted" action} is
     * {@link #WHEN_EXHAUSTED_BLOCK}.
     * <p/>
     * When less than or equal to 0, the {@link #borrowObject} method
     * may block indefinitely.
     *
     * @return maximum number of milliseconds to block when borrowing an object.
     * @see #WHEN_EXHAUSTED_BLOCK
     */
    public synchronized long getMaxWait()
    {
        return maxWait;
    }

    /**
     * Returns the cap on the number of "idle" instances in the pool.
     *
     * @return the cap on the number of "idle" instances in the pool.
     */
    public synchronized int getMaxIdle()
    {
        return maxIdle;
    }

    /**
     * Returns the minimum number of objects allowed in the pool
     * before the evictor thread (if active) spawns new objects.
     * (Note no objects are created when: numActive + numIdle >= maxActive)
     *
     * @return The minimum number of objects.
     */
    public synchronized int getMinIdle()
    {
        return minIdle;
    }

    /**
     * When <tt>true</tt>, objects will be
     * {@link PoolableObjectFactory#validateObject validated}
     * before being returned by the {@link #borrowObject}
     * method.  If the object fails to validate,
     * it will be dropped from the pool, and we will attempt
     * to borrow another.
     *
     * @return <code>true</code> if objects are validated before being borrowed.
     */
    public synchronized boolean getTestOnBorrow()
    {
        return testOnBorrow;
    }

    /**
     * Returns the number of milliseconds to sleep between runs of the
     * idle object evictor thread.
     * When non-positive, no idle object evictor thread will be
     * run.
     *
     * @return number of milliseconds to sleep between evictor runs.
     */
    public synchronized long getTimeBetweenEvictionRunsMillis()
    {
        return timeBetweenEvictionRunsMillis;
    }

    /**
     * Returns the minimum amount of time an object may sit idle in the pool
     * before it is eligible for eviction by the idle object evictor
     * (if any).
     *
     * @return minimum amount of time an object may sit idle in the pool before it is eligible for eviction.
     */
    public synchronized long getMinEvictableIdleTimeMillis()
    {
        return minEvictableIdleTimeMillis;
    }

    /**
     * Returns the minimum amount of time an object may sit idle in the pool
     * before it is eligible for eviction by the idle object evictor
     * (if any), with the extra condition that at least
     * "minIdle" amount of object remain in the pool.
     *
     * @return minimum amount of time an object may sit idle in the pool before it is eligible for eviction.
     */
    public synchronized long getSoftMinEvictableIdleTimeMillis()
    {
        return softMinEvictableIdleTimeMillis;
    }

    private final void assertOpen() throws IllegalStateException
    {
        if(isClosed())
        {
            throw new IllegalStateException("Pool not open");
        }
    }
    //-- ObjectPool methods ------------------------------------------

    public E borrowObject() throws Exception
    {
        assertOpen();
        long starttime = System.currentTimeMillis();
        for (; ;)
        {

            E obj = null;

            // if there are any sleeping, just grab one of those
            synchronized (this)
            {
                obj = pool.remove();

                // otherwise
                if (null == obj)
                {
                    // check if we can create one
                    // (note we know that the num sleeping is 0, else we wouldn't be here)
                    if (maxActive < 0 || numActive < maxActive)
                    {
                        // allow new object to be created
                    }
                    else
                    {
                        // the pool is exhausted
                        switch (whenExhaustedAction)
                        {
                            case WHEN_EXHAUSTED_GROW:
                                // allow new object to be created
                                break;
                            case WHEN_EXHAUSTED_FAIL:
                                throw new NoSuchElementException("Pool exhausted");
                            case WHEN_EXHAUSTED_BLOCK:
                                try
                                {
                                    if (maxWait <= 0)
                                    {
                                        wait();
                                    }
                                    else
                                    {
                                        // this code may be executed again after a notify then continue cycle
                                        // so, need to calculate the amount of time to wait
                                        final long elapsed = (System.currentTimeMillis() - starttime);
                                        final long waitTime = maxWait - elapsed;
                                        if (waitTime > 0)
                                        {
                                            wait(waitTime);
                                        }
                                    }
                                }
                                catch (InterruptedException e)
                                {
                                    Thread.currentThread().interrupt();
                                    throw e;
                                }
                                if (maxWait > 0 && ((System.currentTimeMillis() - starttime) >= maxWait))
                                {
                                    throw new NoSuchElementException("Timeout waiting for idle object");
                                }
                                else
                                {
                                    continue; // keep looping
                                }
                            default:
                                throw new IllegalArgumentException("WhenExhaustedAction property " + whenExhaustedAction + " not recognized.");
                        }
                    }
                }
                numActive++;
            }

            // create new object when needed
            boolean newlyCreated = false;
            if (null == obj)
            {
                try
                {
                    obj = factory.makeObject(this);
                    newlyCreated = true;
                    return obj;
                }
                finally
                {
                    if (!newlyCreated)
                    {
                        // object cannot be created
                        synchronized (this)
                        {
                            numActive--;
                            notifyAll();
                        }
                    }
                }
            }

            // activate & validate the object
            try
            {
                factory.activateObject(obj);
                if (testOnBorrow && !factory.validateObject(obj))
                {
                    throw new Exception("ValidateObject failed");
                }
                return obj;
            }
            catch (Throwable e)
            {
                reportException("object activation or validation failed ", e);
                synchronized (this)
                {
                    // object cannot be activated or is invalid
                    numActive--;
                    notifyAll();
                }
                destroyObject(obj);
                if (newlyCreated)
                {
                    throw new NoSuchElementException("Could not create a validated object, cause: " + e.getMessage());
                }
                // keep looping
            }
        }
    }

    private void destroyObject(E obj)
    {
        try
        {
            factory.destroyObject(obj);
        }
        catch (Throwable t)
        {
            String msg = "object destruction failed ";
            reportException(msg, t);
        }
    }

    private void reportException(String msg, Throwable t)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(msg, t);
        }
        else
        {
            logger.warn(msg +t.getClass().getName()+": "+t.getMessage()+" enable debug for full stack trace");
        }
    }

    public void invalidateObject(E obj) throws Exception
    {
        try
        {
            if (factory != null)
            {
                factory.destroyObject(obj);
            }
        }
        catch (Exception e)
        {
            reportException("object destruction failed ", e);
        }
        finally
        {

            synchronized (this)
            {
                numActive--;
                notifyAll(); // numActive has changed
            }
        }
    }

    /**
     * Clears any objects sitting idle in the pool.
     */
    public synchronized void clear()
    {
        pool.forEachUntil(new DoUntilProcedure<E>()
        {
            public boolean execute(E object)
            {
                destroyObject(object);
                return false;
            }
        });
        pool.clear();
        numActive = 0;
        notifyAll(); // num sleeping has changed
    }

    /**
     * Return the number of instances currently borrowed from this pool.
     *
     * @return the number of instances currently borrowed from this pool
     */
    public synchronized int getNumActive()
    {
        return numActive;
    }

    /**
     * Return the number of instances currently idle in this pool.
     *
     * @return the number of instances currently idle in this pool
     */
    public synchronized int getNumIdle()
    {
        return pool.size();
    }

    /**
     * {@inheritDoc}
     * <p><strong>Note: </strong> There is no guard to prevent an object
     * being returned to the pool multiple times. Clients are expected to
     * discard references to returned objects and ensure that an object is not
     * returned to the pool multiple times in sequence (i.e., without being
     * borrowed again between returns). Violating this contract will result in
     * the same object appearing multiple times in the pool and pool counters
     * (numActive, numIdle) returning incorrect values.</p>
     */
    public void returnObject(E obj) throws Exception
    {
        try
        {
            addObjectToPool(obj, true);
        }
        catch (Exception e)
        {
            reportException("return failed ", e);
        }
    }

    private void addObjectToPool(E obj, boolean decrementNumActive) throws Exception
    {
        boolean success = false;
        try
        {
            factory.passivateObject(obj);
            success = !isClosed();
        }
        finally
        {
            boolean shouldDestroy = !success;

            synchronized (this)
            {
                if (decrementNumActive)
                {
                    numActive--;
                }
                if ((maxIdle >= 0) && (pool.size() >= maxIdle))
                {
                    shouldDestroy = true;
                }
                else if (success)
                {
                    pool.add(obj);
                }
                notifyAll(); // numActive has changed
            }

            if (shouldDestroy)
            {
                destroyObject(obj);
            }
        }
    }

    public void close() throws Exception
    {
        synchronized (this)
        {
            clear();
            startEvictor(-1L);
        }
    }

    /**
     * Make one pass of the idle object evictor.
     *
     * @throws Exception if the pool is closed or eviction fails.
     */
    public void evict() throws Exception
    {
        assertOpen();

        boolean isEmpty;
        synchronized (this)
        {
            isEmpty = pool.isEmpty();
        }
        if (!isEmpty)
        {
            if (softMinEvictableIdleTimeMillis > 0)
            {
                int numToEvict = getNumIdle() - getMinIdle();
                evict(System.currentTimeMillis() - softMinEvictableIdleTimeMillis, numToEvict);
            }
            if (minEvictableIdleTimeMillis > 0)
            {
                int numToEvict = getNumIdle();
                evict(System.currentTimeMillis() - minEvictableIdleTimeMillis, numToEvict);
            }
        }
    }

    private void evict(long lastAccessTime, int maxToEvict)
    {
        if (maxToEvict > 0)
        {
            List<E> toEvict;
            synchronized (this)
            {
                toEvict = pool.removeEvictable(lastAccessTime, maxToEvict);
            }
            for(int i=0;i<toEvict.size();i++)
            {
                destroyObject(toEvict.get(i));
            }
        }
    }

    //--- non-public methods ----------------------------------------

    /**
     * Start the eviction thread or service, or when
     * <i>delay</i> is non-positive, stop it
     * if it is already running.
     *
     * @param delay milliseconds between evictor runs.
     */
    protected synchronized void startEvictor(long delay)
    {
        if (null != evictor)
        {
            evictor.cancel();
            evictor = null;
        }
        if (delay > 0)
        {
            evictor = new Evictor();
            EVICTION_TIMER.schedule(evictor, delay, delay);
        }
    }

    //--- inner classes ----------------------------------------------

    /**
     * The idle object evictor {@link TimerTask}.
     *
     */
    private class Evictor extends TimerTask
    {
        public void run()
        {
            try
            {
                evict();
            }
            catch (Exception e)
            {
                // ignored
            }
        }
    }

}
