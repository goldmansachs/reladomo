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

package com.gs.fw.common.mithra.notification;

import com.gs.fw.common.mithra.list.DelegatingList;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.list.MithraDelegatedList;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;

import java.util.List;
import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;



public class OperationBasedNotificationRegistrationEntry extends WeakReference implements MithraApplicationNotificationRegistrationEntry
{

    private Operation operation;
    private MithraApplicationNotificationListener listener;

    public OperationBasedNotificationRegistrationEntry(Operation operation, List mithraObjectList, MithraApplicationNotificationListener listener, ReferenceQueue queue)
    {
        super(mithraObjectList, queue);
        this.operation = operation;
        this.listener = listener;
    }

    public Operation getOperation()
    {
        return operation;
    }

    public MithraApplicationNotificationListener getListener()
    {
        return listener;
    }

    public void processNotification(MithraNotificationEvent notificationEvent)
    {        
        byte databaseOperation = notificationEvent.getDatabaseOperation();
        DelegatingList mithraObjectList = (DelegatingList) this.get();
        if (mithraObjectList != null)
        {
            PrimaryKeyIndex pkIndex = (PrimaryKeyIndex)(mithraObjectList.zGetNotificationIndex());
            if(databaseOperation != MithraNotificationEvent.INSERT)
            {
                if(databaseOperation == MithraNotificationEvent.MASS_DELETE)
                {
                    listener.deleted();
                }
                else
                {
                    MithraDataObject[] dataObjects = notificationEvent.getDataObjects();

                    for(int i = 0; i < dataObjects.length; i++)
                    {
                        Object obj = pkIndex.getFromData(dataObjects[i]);
                        if(obj != null)
                        {
                            if(notificationEvent.getDatabaseOperation() == MithraNotificationEvent.UPDATE)
                            {
                                listener.updated();
                            }
                            else
                            {
                                listener.deleted();
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
}
