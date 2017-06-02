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

package com.gs.fw.common.mithra.test;

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderDriverFinder;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import junit.framework.TestCase;

import java.util.List;

public class CommonVendorTestCases extends TestCase
{
    public void testRollback()
    {
        final List<Exception> exceptionsList = FastList.newList();
        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TemporaryContext temporaryContext = OrderDriverFinder.createTemporaryContext();

                try
                {
                    Order order = new Order();
                    order.setOrderId(1000);
                    order.insert();
                    if (exceptionsList.isEmpty())
                    {
                        MithraBusinessException exception = new MithraBusinessException("Exception");
                        exception.setRetriable(true);
                        exceptionsList.add(exception);
                        throw exception;
                    }
                    return null;
                }
                finally
                {
                    temporaryContext.destroy();
                }
            }
        }, 5);
        assertNotNull(OrderFinder.findOne(OrderFinder.orderId().eq(1000)));
    }

}
