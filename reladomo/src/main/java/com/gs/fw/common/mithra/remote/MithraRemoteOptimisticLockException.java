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

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;

import java.util.ArrayList;
import java.util.List;


public class MithraRemoteOptimisticLockException extends MithraOptimisticLockException
{
    private ExternalizablePrimaryKeyList dirtyData = new ExternalizablePrimaryKeyList();
    private transient ArrayList dirtyDataList;

    public MithraRemoteOptimisticLockException(String message, boolean retriable)
    {
        super(message, retriable);
        init();
    }

    private void init()
    {
        this.dirtyDataList = new ArrayList();
        dirtyData.setMithraDataObjects(this.dirtyDataList);
    }

    public MithraRemoteOptimisticLockException(String message)
    {
        super(message);
        init();
    }

    public MithraRemoteOptimisticLockException(String message, Throwable nestedException)
    {
        super(message, nestedException);
    }

    public MithraRemoteOptimisticLockException(String message, MithraTransaction transactionToWaitFor)
    {
        super(message, transactionToWaitFor);
    }

    public void addDirtyData(MithraDataObject dirtyData)
    {
        this.dirtyDataList.add(dirtyData);
    }

    public List getDirtyData()
    {
        return dirtyData.getMithraDataObjects();
    }
}
