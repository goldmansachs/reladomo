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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import junit.framework.Assert;

import javax.transaction.Status;
import javax.transaction.Synchronization;


public class MockSynchronization implements Synchronization
{
    private static final Logger logger = LoggerFactory.getLogger(MockSynchronization.class.getName());

    private int beforeCount = 1;
    private int afterCount = 1;
    private int expectedStatus = Status.STATUS_COMMITTED;

    public void setExpectedStatus(int expectedStatus)
    {
        this.expectedStatus = expectedStatus;
    }

    public void afterCompletion(int i)
    {
        if (i == expectedStatus) afterCount--;
        else
        {
            logger.error("unexpected status "+i);
        }
    }

    public void beforeCompletion()
    {
        beforeCount--;
    }

    public void verify()
    {
        Assert.assertEquals(0, beforeCount);
        Assert.assertEquals(0, afterCount);
    }
}
