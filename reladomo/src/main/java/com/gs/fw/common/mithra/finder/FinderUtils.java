
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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.tempobject.TempContextContainer;
import com.gs.fw.common.mithra.tempobject.MithraTemporaryContext;
import com.gs.fw.common.mithra.extractor.NormalAndListValueSelector;

import java.util.List;

public class FinderUtils
{

    public static Object findOne(List found)
    {
        switch(found.size())
        {
            case 0: return null;
            case 1: return found.get(0);
            default:
            {
                String msg = "Primary keys in results:\n";
                for(int i=0;i<found.size();i++)
                {
                    MithraObject o = (MithraObject) found.get(i);
                    MithraDataObject data = o.zGetCurrentData();
                    if (data == null && o instanceof MithraTransactionalObject)
                    {
                        data = ((MithraTransactionalObject)o).zGetTxDataForRead();
                    }
                    msg += data.zGetPrintablePrimaryKey()+"\n";
                }
                throw new MithraBusinessException("findOne returned more than one result for class "+found.get(0).getClass().getName()+
                    "\n"+msg);
            }
        }
    }

    public static Object parentSelectorValueOf(Object incoming, NormalAndListValueSelector parentSelector)
    {
        if (parentSelector != null)
        {
            incoming = parentSelector.valueOf(incoming);
        }
        return incoming;
    }

    public static Object parentSelectorListValueOf(Object incoming, NormalAndListValueSelector parentSelector)
    {
        if (parentSelector != null)
        {
            incoming = parentSelector.listValueOf(incoming);
        }
        return incoming;
    }
    
    public static void clearTempObjectQueryCache(TempContextContainer container)
    {
        MithraTemporaryContext currentContext = container.getCurrentContext();
        if (currentContext != null)
        {
            currentContext.getMithraObjectPortal().clearQueryCache();
        }
    }
}
