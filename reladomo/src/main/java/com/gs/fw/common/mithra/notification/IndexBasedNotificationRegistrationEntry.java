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

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.list.DelegatingList;
import com.gs.fw.common.mithra.list.MithraDelegatedList;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.List;



public class IndexBasedNotificationRegistrationEntry extends WeakReference implements MithraApplicationNotificationRegistrationEntry
{
    static private final Logger logger = LoggerFactory.getLogger(IndexBasedNotificationRegistrationEntry.class.getName());
    private MithraApplicationNotificationListener listener;

    public IndexBasedNotificationRegistrationEntry(List mithraObjectList, MithraApplicationNotificationListener listener, ReferenceQueue queue)
    {
        super(mithraObjectList, queue);
        this.listener = listener;
    }

    public MithraApplicationNotificationListener getListener()
    {
        return listener;
    }

    public void processNotification(MithraNotificationEvent notificationEvent)
    {
        if(logger.isDebugEnabled())
        {
            logger.debug("***************** PROCESSING APPLICATION NOTIFICATION***************************");
        }
        byte databaseOperation = notificationEvent.getDatabaseOperation();
        
        DelegatingList mithraObjectList = (DelegatingList) this.get();
        if (mithraObjectList != null)
        {
            PrimaryKeyIndex pkIndex = (PrimaryKeyIndex)mithraObjectList.zGetNotificationIndex();
            if(databaseOperation != MithraNotificationEvent.INSERT)
            {
                //todo: Review implementation
                if(databaseOperation == MithraNotificationEvent.MASS_DELETE)
                {
                    Operation massDeleteOperation = notificationEvent.getOperationForMassDelete();
                    List mithraObjectsMassDeleted = massDeleteOperation.applyOperation(mithraObjectList);

                    if(mithraObjectsMassDeleted != null && !mithraObjectsMassDeleted.isEmpty())
                    {
                        if(logger.isDebugEnabled())
                        {
                            logger.debug("***************** Notifying Listener that an object from the list was deleted***************************");
                        }
                        listener.deleted();
                    }
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
                                if(logger.isDebugEnabled())
                                {
                                    logger.debug("***************** Notifying Listener that an object from the list was updated***************************");
                                }
                                listener.updated();
                            }
                            else
                            {
                                if(logger.isDebugEnabled())
                                {
                                    logger.debug("***************** Notifying Listener that an object from the list was deleted***************************");
                                }
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
