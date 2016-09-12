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

package com.gs.fw.common.mithra.util;


import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.finder.RelatedFinder;

import java.sql.Timestamp;
import java.util.Comparator;

public class InactivateForArchiveSingleQueueExecutor extends SingleQueueExecutor
{
    private TimestampAttribute[] asOfAttributes = new TimestampAttribute[2];
    public InactivateForArchiveSingleQueueExecutor(int numberOfThreads, Comparator orderBy, int batchSize, RelatedFinder finder, int insertThreads)
    {
        super(numberOfThreads, orderBy, batchSize, finder, insertThreads);
        AsOfAttribute[] finderAsOfAttributes = finder.getAsOfAttributes();
        if (finderAsOfAttributes.length == 2)
        {
            asOfAttributes[0] = finderAsOfAttributes[0].getToAttribute();
            asOfAttributes[1] = finderAsOfAttributes[1].getToAttribute();
        }
        if (finderAsOfAttributes.length == 1)
        {
            if (finderAsOfAttributes[0].isProcessingDate())
            {
                asOfAttributes[0] = null;
                asOfAttributes[1] = finderAsOfAttributes[0].getToAttribute();
            }
        }
    }

    @Override
    protected UpdateOperation createUpdateOperation(MithraTransactionalObject dbObject, MithraTransactionalObject fileObject)
    {
        return new InactiveForArchivingOperation(dbObject, fileObject);
    }

    protected class InactiveForArchivingOperation extends UpdateOperation
    {
        public InactiveForArchivingOperation(MithraTransactionalObject dbObject, MithraTransactionalObject fileObject)
        {
            super(dbObject, fileObject);
        }

        @Override
        public void performOperation(boolean updateWithTerminateAndInsert, Timestamp businessDateTo)
        {
            MithraDatedTransactionalObject datedObject = (MithraDatedTransactionalObject) getDbObject();

            Timestamp inactivateBusinessDateTo = asOfAttributes[0] != null ? asOfAttributes[0].timestampValueOf(getFileObject()) : null;
            datedObject.inactivateForArchiving(asOfAttributes[1].timestampValueOf(getFileObject()), inactivateBusinessDateTo);
        }
    }
}
