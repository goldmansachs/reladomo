/*
  Copyright 2018 Goldman Sachs.
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

package com.gs.reladomo.jms;

import com.gs.fw.common.mithra.MithraTransaction;

/**
The batch processor gets called to setup, process and shutdown the loop.
For large systems that process many incoming queues/topics, a single process can be used
to process many (dozens) queues/topics, each with a loop of its own in its own thread.
*/
public interface BatchProcessor<T extends InFlightBatch, E extends BatchJmsMessageLoop<T>>
{
    //-----------------------   setup methods, called in this order ----------------//
    /**
     * setup outgoing jms topics (if any) by calling connectOutgoingTopicWithRetry.
     * The incoming topic is setup by the loop automatically.
     * The database is treated as non-XA and will get enrolled automatically, so nothing should be done here.
     * Any other XA resources can be setup here (and recovered in recoverNonJmsResources).
     * @param batchJmsMessageLoop
     */
    void setupTopicsAndResources(E batchJmsMessageLoop);

    /**
     * if the batch processor sets up non-jms resources in the setupTopicsAndResources method, it should recover those in this method
     * @param batchJmsMessageLoop
     * @param globalXid id of transaction to recover
     * @param committed whether the in-doubt transaction was committed
     */
    void recoverNonJmsResources(E batchJmsMessageLoop, byte[] globalXid, boolean committed);

    /**
     * Called before the loop starts.
     * @param batchJmsMessageLoop
     */
    void waitTillSourceIsOpen(E batchJmsMessageLoop);

    /**
     * loop is about to start
     * @param batchJmsMessageLoop
     */
    void notifyLoopReady(E batchJmsMessageLoop);

    //----------------------- loop methods, called over and over in this order ----------//
    /**
     * return true if you want the loop to end -- very useful for tests or loops that need to end for some reason
     * @param batchJmsMessageLoop
     * @return
     */
    boolean shutdownNow(E batchJmsMessageLoop);

    /**
     * This method can wait for an external event before we start the transaction (to minimize the time in transaction).
     * @param batchJmsMessageLoop
     */
    void preTransaction(E batchJmsMessageLoop);

    /**
     * Usually large reference data that's only read, can be set to optimistic locking.
     * Called right after the transaction is started.
     * @param loop
     * @param tx
     */
    void setupOptimisticReads(E loop, MithraTransaction tx);

    /**
     * Enlist any non-jms resources that were setup in the setupTopicsAndResources method.
     * Called just before we start reading the incoming messages.
     * @param batchJmsMessageLoop
     */
    void enlistNonJmsResources(E batchJmsMessageLoop);

    /**
     * Factory method for the InFlightBatch. Called before we read any messages.
     * @param batchJmsMessageLoop
     * @return
     */
    T createInFlightBatch(BatchJmsMessageLoop<T> batchJmsMessageLoop);

    /**
     * At this point, messages are repeatedly read from the incoming jms and inFlightBatch is populated via addMessage
     * After each message is read, we call this method to see if we should end the loop or keep on reading.
     * Even if this method returns false, the message reading will stop if max batch size is reached,
     * or no message was available after a short timeout.
     * @param batchJmsMessageLoop
     * @param inFlightBatch
     * @return
     */
    boolean isLastMessageEndOfBatch(E batchJmsMessageLoop, T inFlightBatch);

    /**
     * Message reading is finished, now do the bulk of the transactional work for these records.
     * @param batchJmsMessageLoop
     * @param inFlightBatch
     */
    void processRecords(E batchJmsMessageLoop, T inFlightBatch);

    /**
     * Commit was successful. Do any post commit work here.
     * @param batchJmsMessageLoop
     * @param inFlightBatch
     */
    void processPostCommit(E batchJmsMessageLoop, T inFlightBatch);

    // if an exception happens during the loop, the following methods are called
    // exceptions during processing will cause the batch size to reduce, eventually down to one.
    /**
     * this batch was rolled back. Do any post roll back cleanup here.
     * @param batchJmsMessageLoop
     * @param inFlightBatch
     */
    void transactionRolledBack(E batchJmsMessageLoop, T inFlightBatch);

    /**
     * If the batch had only one message in it, after an exception happens,
     * this method is called to make sure things are not completely bad.
     * Typically, it's good to do a database query to make sure the db is up and running and configured properly.
     * Should throw an exception if something is wrong.
     * @param batchJmsMessageLoop
     * @throws Exception
     */
    void sanityCheck(E batchJmsMessageLoop) throws Exception;

    /**
     * if sanityCheck does not throw an exception, this method is called.
     * it might be necessary to set the loop as "poisoned" after a threshold of rejections.
     *
     * @param batchJmsMessageLoop
     * @param consequetiveUnhandledRejections
     * @param e Cause of the rollback
     * @param inFlightBatch This is guaranteed to have just one message in it.
     */
    void handleUnexpectedRejection(E batchJmsMessageLoop, int consequetiveUnhandledRejections, Throwable e, T inFlightBatch);

    /**
     * Do any work at the bottom of the loop, after the transaction is committed or rolledback.
     * If the loop committed successfully, inFlightBatch will not be null.
     * If the loop rolledback, inFlightBatch will be null.
     * This method can potentially wait, waiting for some external event (if applicable).
     *
     * @param batchJmsMessageLoop
     * @param inFlightBatch null if the loop rolled back.
     */
    void postTransaction(E batchJmsMessageLoop, T inFlightBatch);

    //----------------  following methods are called when the loop is shutdown --------------------------

    /**
     * called at the start of shutdown.
     * @param batchJmsMessageLoop
     */
    void shutdownStart(E batchJmsMessageLoop);

    /**
     * close any non-jms resources that was setup in setupTopicsAndResources
     * @param batchJmsMessageLoop
     */
    void closeNonJmsResources(E batchJmsMessageLoop);

}
