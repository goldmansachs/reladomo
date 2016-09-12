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

package com.gs.fw.common.mithra.test.util.tinyproxy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invocation handler for stream based transports (http, socket, etc).
 */
public class StreamBasedInvocator
{
    public static final byte OK_STATUS = (byte) 0;
    public static final byte FAULT_STATUS = (byte) 1;
    public static final byte REQUEST_NEVER_ARRVIED_STATUS = (byte) 2;

    public static final byte INVOKE_REQUEST = (byte) 0;
    public static final byte RESEND_REQUEST = (byte) 1;
    public static final byte THANK_YOU_REQUEST = (byte) 2;
    public static final byte PING_REQUEST = (byte) 3;
    public static final byte INIT_REQUEST = (byte) 4;

    //private static final boolean CAUSE_RANDOM_ERROR = true;
    private static final double ERROR_RATE = 0.98;

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamBasedInvocator.class.getName());

    private boolean abortInvocation;

    /**
     * Invoke the object with the request from the input stream.
     *  @param in      the object input stream
     * @param context the invocation context*/
    public void invoke(
            ObjectInputStream in,
            Context context,
            Object service,
            MethodResolver methodResolver,
            String remoteAddress,
            RequestId requestId) throws Exception
    {
        boolean continueInvocation = true;
        synchronized (context)
        {
            this.abortInvocation = false;
            // check to see if there is another request for the same invocation.
            if (context.isInvokingMethod())
            {
                continueInvocation = false;
                context.wait(); // just wait for the other one
            }
            else if (context.isInvocationFinished())
            {
                continueInvocation = false;
            }
            else
            {
                context.setReadingParametersState(this);
            }
        }
        //if (CAUSE_RANDOM_ERROR) if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
        String methodName = (String) in.readObject();
        Method method = methodResolver.getMethodFromMangledName(methodName);

        if (method == null)
        {
            throw new IOException("No server method matching:" + methodName);
        }

        Class[] args = method.getParameterTypes();
        Object[] values = new Object[args.length];

        for (int i = 0; i < args.length; i++)
        {
            //if (CAUSE_RANDOM_ERROR) if (Math.random() > ERROR_RATE) throw new IOException("Random error, for testing only!");
            values[i] = in.readObject();
        }
        if (continueInvocation)
        {
            context.setInvokingMethodState(this);

            if (!this.abortInvocation)
            {
                long start = System.currentTimeMillis();
                try
                {
                    Object result = method.invoke(service, values);
                    context.setReturnValue(result, false);
                }
                catch (Throwable e)
                {
                    if (e instanceof InvocationTargetException)
                    {
                        e = ((InvocationTargetException) e).getTargetException();
                    }
                    LOGGER.error("an exception occured while invoking {}", method.getName(), e);
                    context.setReturnValue(e, true);

                }
                finally
                {
                    long currentTime = System.currentTimeMillis();
                    if (LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Invoking method "+method.getDeclaringClass().getName()+"."+method.getName()+" took "+(currentTime - start)+") ms");
                    }
                }
            }
        }
    }

    public void setAbortInvocation()
    {
        this.abortInvocation = true;
    }
}
