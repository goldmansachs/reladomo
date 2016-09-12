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

package com.gs.fw.common.mithra.behavior;

import com.gs.fw.common.mithra.behavior.deleted.DatedDeletedBehavior;
import com.gs.fw.common.mithra.behavior.deleted.DatedDeletedDifferentTxBehavior;
import com.gs.fw.common.mithra.behavior.deleted.DatedDeletedThreadNoTxObjectTxBehavior;
import com.gs.fw.common.mithra.behavior.detached.*;
import com.gs.fw.common.mithra.behavior.inmemory.*;
import com.gs.fw.common.mithra.behavior.persisted.*;



public class AbstractDatedTransactionalBehavior
{
    private static final DatedPersistedSameTxBehavior PERSISTED_SAME_TX_BEHAVIOR = new DatedPersistedSameTxBehavior();
    private static final DatedPersistedNoTxBehavior PERSISTED_NO_TX_BEHAVIOR = new DatedPersistedNoTxBehavior();
    private static final DatedPersistedNonTransactionalBehavior PERSISTED_NON_TRANSACTIONAL_BEHAVIOR = new DatedPersistedNonTransactionalBehavior();
    private static final DatedPersistedDifferentTxBehavior PERSISTED_DIFFERENT_TX_BEHAVIOR = new DatedPersistedDifferentTxBehavior();
    private static final DatedPersistedThreadNoTxObjectTxBehavior PERSISTED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR = new DatedPersistedThreadNoTxObjectTxBehavior();
    private static final DatedPersistedTxEnrollBehavior PERSISTED_TX_ENROLL_BEHAVIOR = new DatedPersistedTxEnrollBehavior();

    private static final DatedInMemorySameTxBehavior IN_MEMORY_SAME_TX_BEHAVIOR = new DatedInMemorySameTxBehavior();
    private static final DatedInMemoryNoTxBehavior IN_MEMORY_NO_TX_BEHAVIOR = new DatedInMemoryNoTxBehavior();

    private static final DatedInMemoryNonTransactionalBehavior DATED_IN_MEMORY_NON_TRANSACTIONAL_BEHAVIOR = new DatedInMemoryNonTransactionalBehavior();

    private static final DatedInMemoryDifferentTxBehavior IN_MEMORY_DIFFERENT_TX_BEHAVIOR = new DatedInMemoryDifferentTxBehavior();
    private static final DatedInMemoryThreadNoTxObjectTxBehavior IN_MEMORY_THREAD_NO_TX_OBJECT_TX_BEHAVIOR = new DatedInMemoryThreadNoTxObjectTxBehavior();
    private static final DatedInMemoryTxEnrollBehavior IN_MEMORY_TX_ENROLL_BEHAVIOR = new DatedInMemoryTxEnrollBehavior();

    private static final DatedDetachedSameTxBehavior DETACHED_SAME_TX_BEHAVIOR = new DatedDetachedSameTxBehavior();
    private static final DatedDetachedNoTxBehavior DETACHED_NO_TX_BEHAVIOR = new DatedDetachedNoTxBehavior();
    private static final DatedDetachedTxEnrollBehavior DETACHED_TX_ENROLL_BEHAVIOR = new DatedDetachedTxEnrollBehavior();

    private static final DatedDeletedBehavior DEFAULT_DELETED_BEHAVIOR = new DatedDeletedBehavior();
    private static final DatedDeletedDifferentTxBehavior DELETED_DIFFERENT_TX_BEHAVIOR = new DatedDeletedDifferentTxBehavior();
    private static final DatedDeletedThreadNoTxObjectTxBehavior DELETED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR = new DatedDeletedThreadNoTxObjectTxBehavior();

    private static final DatedDetachedDeletedSameTxBehavior DETACHED_DELETED_SAME_TX_BEHAVIOR = new DatedDetachedDeletedSameTxBehavior();
    private static final DatedDetachedDeletedNoTxBehavior DETACHED_DELETED_NO_TX_BEHAVIOR = new DatedDetachedDeletedNoTxBehavior();
    private static final DatedDetachedDeletedDifferentTxBehavior DETACHED_DELETED_DIFFERENT_TX_BEHAVIOR = new DatedDetachedDeletedDifferentTxBehavior();
    private static final DatedDetachedDeletedThreadNoTxObjectTxBehavior DETACHED_DELETED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR = new DatedDetachedDeletedThreadNoTxObjectTxBehavior();
    private static final DatedDetachedDeletedTxEnrollBehavior DETACHED_DELETED_TX_ENROLL_BEHAVIOR = new DatedDetachedDeletedTxEnrollBehavior();

    public static final DatedPersistedSameTxBehavior getPersistedSameTxBehavior()
    {
        return PERSISTED_SAME_TX_BEHAVIOR;
    }

    public static DatedInMemoryNonTransactionalBehavior getDatedInMemoryNonTransactionalBehavior()
    {
        return DATED_IN_MEMORY_NON_TRANSACTIONAL_BEHAVIOR;
    }

    public static DatedInMemorySameTxBehavior getInMemorySameTxBehavior()
    {
        return IN_MEMORY_SAME_TX_BEHAVIOR;
    }

    public static DatedPersistedNonTransactionalBehavior getPersistedNonTransactionalBehavior()
    {
        return PERSISTED_NON_TRANSACTIONAL_BEHAVIOR;
    }

    public static DatedPersistedNoTxBehavior getPersistedNoTxBehavior()
    {
        return PERSISTED_NO_TX_BEHAVIOR;
    }

    public static DatedPersistedDifferentTxBehavior getPersistedDifferentTxBehavior()
    {
        return PERSISTED_DIFFERENT_TX_BEHAVIOR;
    }

    public static DatedPersistedThreadNoTxObjectTxBehavior getPersistedThreadNoTxObjectTxBehavior()
    {
        return PERSISTED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR;
    }

    public static DatedPersistedTxEnrollBehavior getPersistedTxEnrollBehavior()
    {
        return PERSISTED_TX_ENROLL_BEHAVIOR;
    }

    public static DatedInMemoryNoTxBehavior getInMemoryNoTxBehavior()
    {
        return IN_MEMORY_NO_TX_BEHAVIOR;
    }

    public static DatedInMemoryDifferentTxBehavior getInMemoryDifferentTxBehavior()
    {
        return IN_MEMORY_DIFFERENT_TX_BEHAVIOR;
    }

    public static DatedInMemoryThreadNoTxObjectTxBehavior getInMemoryThreadNoTxObjectTxBehavior()
    {
        return IN_MEMORY_THREAD_NO_TX_OBJECT_TX_BEHAVIOR;
    }

    public static DatedInMemoryTxEnrollBehavior getInMemoryTxEnrollBehavior()
    {
        return IN_MEMORY_TX_ENROLL_BEHAVIOR;
    }

    public static DatedDeletedBehavior getDefaultDeletedBehavior()
    {
        return DEFAULT_DELETED_BEHAVIOR;
    }

    public static DatedDeletedDifferentTxBehavior getDeletedDifferentTxBehavior()
    {
        return DELETED_DIFFERENT_TX_BEHAVIOR;
    }

    public static DatedDeletedThreadNoTxObjectTxBehavior getDeletedThreadNoTxObjectTxBehavior()
    {
        return DELETED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR;
    }

    public static DatedDetachedSameTxBehavior getDetachedSameTxBehavior()
    {
        return DETACHED_SAME_TX_BEHAVIOR;
    }

    public static DatedDetachedNoTxBehavior getDetachedNoTxBehavior()
    {
        return DETACHED_NO_TX_BEHAVIOR;
    }

    public static DatedDetachedTxEnrollBehavior getDetachedTxEnrollBehavior()
    {
        return DETACHED_TX_ENROLL_BEHAVIOR;
    }

    public static DatedDetachedDeletedSameTxBehavior getDetachedDeletedSameTxBehavior()
    {
        return DETACHED_DELETED_SAME_TX_BEHAVIOR;
    }

    public static DatedDetachedDeletedNoTxBehavior getDetachedDeletedNoTxBehavior()
    {
        return DETACHED_DELETED_NO_TX_BEHAVIOR;
    }

    public static DatedDetachedDeletedDifferentTxBehavior getDetachedDeletedDifferentTxBehavior()
    {
        return DETACHED_DELETED_DIFFERENT_TX_BEHAVIOR;
    }

    public static DatedDetachedDeletedThreadNoTxObjectTxBehavior getDetachedDeletedThreadNoTxObjectTxBehavior()
    {
        return DETACHED_DELETED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR;
    }

    public static DatedDetachedDeletedTxEnrollBehavior getDetachedDeletedTxEnrollBehavior()
    {
        return DETACHED_DELETED_TX_ENROLL_BEHAVIOR;
    }
}
