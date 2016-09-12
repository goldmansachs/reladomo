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

package com.gs.fw.common.mithra.test.localtx;

import junit.framework.Assert;

import java.util.ArrayList;
import java.util.List;


public class ExecutionOrder
{

    private static volatile int order = 0;
    private static ThreadLocal callOrder = new ThreadLocal();

    private List calledMethods = new ArrayList();
    
    public static synchronized final int getOrder()
    {
        return order++;
    }

    public static void start()
    {
        callOrder.set(new ExecutionOrder());
    }

    public static void addMethod(Object o, String methodName)
    {
        ExecutionOrder executionOrder = getExecutionOrder();
        executionOrder.calledMethods.add(new ObjectAndMethod(o, methodName));
    }

    public static ExecutionOrder getExecutionOrder()
    {
        ExecutionOrder executionOrder = (ExecutionOrder) callOrder.get();
        return executionOrder;
    }

    public static void verifyForThread(Object o, String methodName)
    {
        ExecutionOrder executionOrder = getExecutionOrder();
        executionOrder.verify(o, methodName);
    }

    public void verify(Object o, String methodName)
    {
        if (calledMethods.isEmpty())
        {
            Assert.fail("method " + methodName+" on "+o+" never called");
        }
        ObjectAndMethod oAndM = (ObjectAndMethod) this.calledMethods.remove(0);
        oAndM.verify(o, methodName);
    }

    private static class ObjectAndMethod
    {
        private Object o;
        private String method;

        public ObjectAndMethod(Object o, String method)
        {
            this.o = o;
            this.method = method;
        }

        public void verify(Object o, String methodName)
        {
            if (o != this.o || !this.method.equals(methodName))
            {
                Assert.fail("method "+methodName+" not called on "+o+" instead, called "+this.method+" on object "+this.o);
            }
        }
    }
}
