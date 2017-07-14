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

package com.gs.fw.common.mithra;


import com.gs.fw.common.mithra.tempobject.CommonTempContext;
import com.gs.fw.common.mithra.util.SmallSet;
import org.slf4j.Logger;

import java.util.Set;

public class MithraBusinessException extends MithraException
{
    private boolean isRetriable;
    private boolean isTimedOut;
    private transient Set<CommonTempContext> contexts;

    public MithraBusinessException(String message)
    {
        super(message);
    }

    public MithraBusinessException(String message, Throwable nestedException)
    {
        super(message, nestedException);
    }

    public boolean isRetriable()
    {
        return this.isRetriable;
    }

    public void setRetriable(boolean retriable)
    {
        this.isRetriable = retriable;
    }

    // must not be called from within a transaction, unless the work is being done async in a non-tx thread.
    public int ifRetriableWaitElseThrow(String msg, int retriesLeft, Logger logger)
    {
        if (this.isRetriable() && --retriesLeft > 0)
        {
            logger.warn(msg+ " " + this.getMessage());
            if (logger.isDebugEnabled())
            {
                logger.debug("find failed with retriable error. retrying.", this);
            }
            cleanupAndRecreateTempContexts();
            this.waitBeforeRetrying();
        }
        else throw this;
        return retriesLeft;
    }

    private void cleanupAndRecreateTempContexts()
    {
        if (contexts != null)
        {
            for(CommonTempContext context: contexts)
            {
                context.cleanupAndRecreate();
            }
        }
    }

    public boolean isTimedOut()
    {
        return this.isTimedOut;
    }

    public void setTimedOut(boolean timedOut)
    {
        this.isTimedOut = timedOut;
    }

    public void waitBeforeRetrying()
    {
        MithraManagerProvider.getMithraManager().sleepBeforeTransactionRetry();
    }

    public void addContextsForRetry(Set<CommonTempContext> contexts)
    {
        if (contexts.size() > 0)
        {
            if (this.contexts == null)
            {
                this.contexts = new SmallSet(2);
            }
            this.contexts.addAll(contexts);
        }
    }
}
