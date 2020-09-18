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

package com.gs.fw.common.mithra.test.multivm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;



public class RemoteWorkerVmImpl implements RemoteWorkerVm
{
    private static  Logger logger = LoggerFactory.getLogger(RemoteWorkerVmImpl.class.getName());

    private static MultiVmTest runningTest;
    private static final Class[] EMPTY_CLASS_ARRAY = new Class[0];
    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private static WorkerVmHeartbeat heartbeat;

    public RemoteWorkerVmImpl()
    {
        if (heartbeat == null)
        {
            heartbeat = new WorkerVmHeartbeat();
            heartbeat.start();
        }
    }

    public static void exitWorkerVm()
    {
        synchronized(heartbeat)
        {
            heartbeat.setDone(true);
            heartbeat.notify();
        }
    }

    public void ping()
    {
        this.heartbeat.setLastPingTime(System.currentTimeMillis());
    }

    public Object executeMethod(String methodName)
    {
        try
        {
            this.logger.debug("executing method "+methodName);
            Method method = runningTest.getClass().getMethod(methodName, EMPTY_CLASS_ARRAY);
            return method.invoke(runningTest, EMPTY_OBJECT_ARRAY);
        }
        catch(InvocationTargetException ite)
        {
            Throwable t = ite.getTargetException();
            if (t instanceof Error) throw (Error) t;
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            throw new RemoteWorkerVmException("Could not execute method"+methodName+" on class "+runningTest.getClass().getName(), ite);
        }
        catch(Error e)
        {
            throw e;
        }
        catch(RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            logger.error("Could not execute method "+methodName+" on class "+runningTest.getClass().getName(), e);
            throw new RemoteWorkerVmException("Could not execute method "+methodName+" on class "+runningTest.getClass().getName(), e);
        }
    }

    public Object executeMethod(String methodName, Class[] argumentTypes, Object[] args)
    {
        try
        {
            this.logger.debug("executing method "+methodName);
            Method method = runningTest.getClass().getMethod(methodName, argumentTypes);
            return method.invoke(runningTest, args);
        }
        catch(InvocationTargetException ite)
        {
            Throwable t = ite.getTargetException();
            if (t instanceof Error) throw (Error) t;
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            throw new RemoteWorkerVmException("Could not execute method"+methodName+" on class "+runningTest.getClass().getName(), ite);
        }
        catch(Error e)
        {
            throw e;
        }
        catch(RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            logger.error("Could not execute method "+methodName+" on class "+runningTest.getClass().getName(), e);
            throw new RemoteWorkerVmException("Could not execute method "+methodName+" on class "+runningTest.getClass().getName(), e);
        }
    }

    public static void setTestCase(MultiVmTest testCase)
    {
        runningTest = testCase;
    }

    private static class WorkerVmHeartbeat extends Thread
    {
        private boolean done = false;
        private long lastPingTime;

        public WorkerVmHeartbeat()
        {
            this.setDaemon(false);
            this.setPriority(Thread.MIN_PRIORITY);
            this.lastPingTime = System.currentTimeMillis();
        }

        public void setLastPingTime(long lastPingTime)
        {
            this.lastPingTime = lastPingTime;
        }

        public void setDone(boolean done)
        {
            this.done = done;
        }

        public void run()
        {
            while(!done)
            {
                try
                {
                    synchronized(this)
                    {
                        this.wait(WorkerVm.HEARTBEAT_INTERVAL_MS);
                        if (lastPingTime < System.currentTimeMillis() - WorkerVm.PING_INTERVAL_MS * 5)
                        {
                            System.out.println("Forcing WorkerVm shutdown.");
                            System.exit(-1);
                        }
                    }
                }
                catch(Throwable e)
                {
                    // ignore
                }
            }
        }
    }

}
