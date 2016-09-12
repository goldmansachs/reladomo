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



public class MithraBusinessException extends MithraException
{
    private boolean isRetriable;
    private boolean isTimedOut;

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
}
