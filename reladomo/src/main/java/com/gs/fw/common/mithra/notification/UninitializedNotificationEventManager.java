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
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationClassLevelNotificationListener;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.transaction.MultiUpdateOperation;

import java.util.List;
import java.util.Map;



public class UninitializedNotificationEventManager implements MithraNotificationEventManager
{

    public void registerForNotification(String subject, MithraObjectPortal portal)
    {

    }

    public void registerForApplicationNotification(String subject, MithraApplicationNotificationListener listener,
                             RelatedFinder finder, List mithraObjectList, Operation operation)
    {

    }

    public void registerForApplicationClassLevelNotification(String subject, MithraApplicationClassLevelNotificationListener listener, RelatedFinder finder)
    {

    }

    public Map getMithraListToNotificationListenerMap()
    {
        return null;
    }

    public Map getMithraListToDatabaseIdentiferMap()
    {
        return null;  
    }

    public void broadcastNotificationMessage(Map notificationEvents, long requestorVmId)
    {

    }

    public void processNotificationEvents(String subject, List<MithraNotificationEvent> notificationEvents)
    {

    }

    public void addMithraNotificationEvent(String databaseIdentifier, String classname, byte databaseOperation, MithraDataObject mithraDataObject, Object sourceAttribute)
    {

    }

    public void addMithraNotificationEventForUpdate(String databaseIdentifier, String classname, byte databaseOperation, MithraDataObject mithraDataObject, AttributeUpdateWrapper updateWrapper, Object sourceAttribute)
    {

    }

    public void addMithraNotificationEventForUpdate(String databaseIdentifier, String classname, byte databaseOperation, MithraDataObject mithraDataObject, List updateWrappers, Object sourceAttribute)
    {

    }

    public void addMithraNotificationEvent(String databaseIdentifier, String classname, byte databaseOperation, List mithraObjects, Object sourceAttribute)
    {

    }

    public void addMithraNotificationEventForBatchUpdate(String databaseIdentifier, String classname, byte databaseOperation, List updateOperations, List updateWrappers, Object sourceAttribute)
    {

    }

    public void addMithraNotificationEventForMultiUpdate(String databaseIdentifier, String classname, byte databaseOperation, MultiUpdateOperation multiUpdateOperation, Object sourceAttribute)
    {
        
    }

    public void addMithraNotificationEventForMassDelete(String databaseIdentifier, String classname, byte databaseOperation, Operation operationForMassDelete)
    {

    }


    public void clearNotificationSubscribers()
    {

    }

    public void waitUntilCurrentNotificationTasksAreDone()
    {
        // nothing to do
    }

    public List getNotificationSubscribers()
    {
        return null;
    }

    public void shutdown()
    {

    }

    public boolean isQueuedExecutorChannelEmpty()
    {
        return true;
    }

    public void forceSendNow()
    {
    }

    public long getMithraVmId()
    {
        return 0;
    }

    public long getProcessedMessagesCount()
    {
        return 0;
    }
}
