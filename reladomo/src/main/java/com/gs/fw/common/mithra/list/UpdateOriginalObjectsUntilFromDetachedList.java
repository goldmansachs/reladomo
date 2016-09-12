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

package com.gs.fw.common.mithra.list;

import com.gs.fw.common.mithra.*;

import java.sql.Timestamp;
import java.util.List;



public class UpdateOriginalObjectsUntilFromDetachedList implements TransactionalCommand
{
    private List toUpdateOrInsertList;
    private List toDeleteList;
    private MithraList list;
    private Timestamp until;

    public UpdateOriginalObjectsUntilFromDetachedList(List toUpdateOrInsertList, List toDeleteList, MithraList list, Timestamp until)
    {
        this.toUpdateOrInsertList = toUpdateOrInsertList;
        this.toDeleteList = toDeleteList;
        this.list = list;
        this.until = until;
    }

    public Object executeTransaction(MithraTransaction tx) throws Throwable
    {
        if (list != null)
        {
            list.forceResolve();
        }
        // todo: get the originals in one go to reduce selects when no operation is specified
        delete();
        update();
        insert();
        return null;
    }

    private void insert()
    {
        for(int i=0;i<toUpdateOrInsertList.size();i++)
        {
            MithraDatedTransactionalObject obj = (MithraDatedTransactionalObject) toUpdateOrInsertList.get(i);
            if (!obj.zIsDetached())
            {
                obj.copyDetachedValuesToOriginalOrInsertIfNewUntil(until);
            }
        }
    }

    private void delete()
    {
        if (this.toDeleteList != null)
        {
            for(int i=0;i<toDeleteList.size();i++)
            {
                MithraDatedTransactionalObject obj = (MithraDatedTransactionalObject) toDeleteList.get(i);
                obj.copyDetachedValuesToOriginalOrInsertIfNewUntil(until);
            }
        }
    }

    private void update()
    {
        for(int i=0;i<toUpdateOrInsertList.size();i++)
        {
            MithraDatedTransactionalObject obj = (MithraDatedTransactionalObject) toUpdateOrInsertList.get(i);
            if (obj.zIsDetached())
            {
                obj.copyDetachedValuesToOriginalOrInsertIfNewUntil(until);
            }
        }
    }
}
