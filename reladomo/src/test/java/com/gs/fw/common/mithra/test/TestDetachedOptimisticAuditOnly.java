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


import com.gs.fw.common.mithra.test.domain.AuditedManufacturer;
import com.gs.fw.common.mithra.test.domain.AuditedManufacturerFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItem;
import com.gs.fw.common.mithra.test.domain.AuditedOrderItemFinder;
import com.gs.fw.common.mithra.test.domain.AuditedOrderStatus;
import java.text.ParseException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.OrderTestResultSetComparator;
import com.gs.fw.common.mithra.test.domain.dated.AuditedOrderStatusTwo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDetachedOptimisticAuditOnly extends MithraTestAbstract
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestDetachedOptimisticAuditOnly.class);

    @Override
    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            AuditedOrder.class,
            AuditedOrderItem.class,
            AuditedOrderStatus.class,
            AuditedOrderStatusTwo.class,
            AuditedManufacturer.class,
        };
    }


    public void testWithMultipleTransactionsBothHaveUDFsAndExistingSummaries()
            throws InterruptedException, ParseException
    {
        MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();

        AuditedOrderItemFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
        AuditedOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
        AuditedManufacturerFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);

        tx.setTransactionName("Main Transaction");
        LOGGER.debug("Main thread - started transaction : " + tx.getTransactionName());
        int orderItemId = 1;
        AuditedOrderItem detached1 = AuditedOrderItemFinder.findOne(AuditedOrderItemFinder.id().eq(orderItemId)).getDetachedCopy();

        Semaphore semaphore1 = new Semaphore(1);
        semaphore1.acquire();

        Semaphore semaphore2 = new Semaphore(1);
        semaphore2.acquire();

        AtomicInteger indicator = new AtomicInteger();

        Thread thread2 = this.getOrderDetailsInSeparateThreadAndTransaction(semaphore1, 3, 2, indicator);
        Thread thread3 = this.getOrderDetailsInSeparateThreadAndTransaction(semaphore2, 5, 3, indicator);

        //go through relationship via order item to manufacturer
        LOGGER.debug("Main thread  start of second fetch of product");
        String manufacturerName = detached1.getOrder().getItems().get(0).getManufacturer().getName();

        LOGGER.debug("main thread -  manufacturer = " + manufacturerName);
        LOGGER.debug("thread 1 - pausing");
        sleep(100);
        LOGGER.debug("Main thread - finished pausing, releasing semaphore");
        semaphore1.release();
        LOGGER.debug("Main thread - pausing again");
        sleep(100);
        LOGGER.debug("Main thread - finished pausing");

        semaphore2.release();
        tx.commit();
        LOGGER.debug("Main thread - transaction committed : " + tx.getTransactionName());
        thread2.join();
        thread3.join();

        assertEquals("if not 0, a runtime exception happened", 0, indicator.get());
    }

    public Thread getOrderDetailsInSeparateThreadAndTransaction(
            final Semaphore semaphore,
            final int orderItemId,
            final int threadNumber,
            final AtomicInteger indicator) throws InterruptedException
    {
        final Runnable updater = new Runnable()
        {
            @Override
            public void run()
            {
                LOGGER.debug("Thread " + threadNumber + " started");
                AuditedOrderItem detached = AuditedOrderItemFinder.findOne(AuditedOrderItemFinder.id().eq(orderItemId)).getDetachedCopy();

                MithraTransaction tx = MithraManager.getInstance().startOrContinueTransaction();
                AuditedOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                AuditedOrderItemFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                AuditedManufacturerFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);

                tx.setTransactionName("txn" + threadNumber);
                LOGGER.debug("thread" + threadNumber + " - started transaction : " + tx.getTransactionName());

                LOGGER.debug("Thread " + threadNumber + " - waiting to acquire semaphore");
                try
                {
                    semaphore.acquire();
                    LOGGER.debug("Thread " + threadNumber + " - semaphore acquired");

                    //go through relationship to order
                    LOGGER.debug("Thread " + threadNumber + " start of second fetch of product code");
                    String manufacturerName = detached.getOrder().getItems().get(0).getManufacturer().getName();

                    LOGGER.debug("Thread " + threadNumber + " - product code : " + manufacturerName);

                    LOGGER.debug("Thread " + threadNumber + "sleeping");
                    sleep(100);
                    LOGGER.debug("Thread " + threadNumber + "awake");
                }
                catch (RuntimeException re)
                {
                    LOGGER.error("Unexpected:", re);
                    indicator.getAndAdd(1);
                }
                catch (InterruptedException ie)
                {
                    LOGGER.error("Unexpected:", ie);
                }

                tx.commit();
                LOGGER.debug("Thread " + threadNumber + " - transaction committed : " + tx.getTransactionName());
            }
        };
        Thread anotherThread = new Thread(updater, "Thread number: " + threadNumber + " order ID : " + orderItemId);
        anotherThread.start();
        return anotherThread;
    }
}
