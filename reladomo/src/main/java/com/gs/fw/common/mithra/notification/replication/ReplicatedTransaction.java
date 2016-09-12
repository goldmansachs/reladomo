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

package com.gs.fw.common.mithra.notification.replication;

import com.gs.fw.common.mithra.notification.RunsMasterQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ReplicatedTransaction implements Comparable
{

    private List events = new ArrayList();
    private byte[] txId;
    private int firstEventId;
    private boolean isContiguous = true;
    private boolean sawUnmatchedTransaction = false;

    public ReplicatedTransaction(RunsMasterQueue event)
    {
        events.add(event);
        this.txId = event.getTranId();
        this.firstEventId = event.getEventId();
    }

    public boolean add(RunsMasterQueue other)
    {
        if (Arrays.equals(other.getTranId(), this.txId))
        {
            events.add(other);
            if (sawUnmatchedTransaction)
            {
                this.isContiguous = false;
            }
            return true;
        }
        this.sawUnmatchedTransaction = true;
        return false;
    }

    public boolean isContiguous()
    {
        return isContiguous;
    }

    public List getEvents()
    {
        return events;
    }

    public int compareTo(Object o)
    {
        ReplicatedTransaction rt = (ReplicatedTransaction) o;
        return this.firstEventId  - rt.firstEventId;
    }
}
