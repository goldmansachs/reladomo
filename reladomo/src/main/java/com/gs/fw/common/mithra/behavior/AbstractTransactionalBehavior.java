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

import com.gs.fw.common.mithra.behavior.deleted.DeletedBehavior;
import com.gs.fw.common.mithra.behavior.deleted.DeletedDifferentTxBehavior;
import com.gs.fw.common.mithra.behavior.deleted.DeletedThreadNoTxObjectTxBehavior;
import com.gs.fw.common.mithra.behavior.detached.*;
import com.gs.fw.common.mithra.behavior.inmemory.*;
import com.gs.fw.common.mithra.behavior.persisted.*;



public class AbstractTransactionalBehavior
{

    private static final PersistedSameTxBehavior PERSISTED_SAME_TX_BEHAVIOR = new PersistedSameTxBehavior();
    private static final PersistedNoTxBehavior PERSISTED_NO_TX_BEHAVIOR = new PersistedNoTxBehavior();
    private static final PersistedNonTransactionalBehavior PERSISTED_NON_TRANSACTIONAL_BEHAVIOR = new PersistedNonTransactionalBehavior();
    private static final PersistedDifferentTxBehavior PERSISTED_DIFFERENT_TX_BEHAVIOR = new PersistedDifferentTxBehavior();
    private static final PersistedThreadNoTxObjectTxBehavior PERSISTED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR = new PersistedThreadNoTxObjectTxBehavior();
    private static final PersistedTxEnrollBehavior PERSISTED_TX_ENROLL_BEHAVIOR = new PersistedTxEnrollBehavior();

    private static final InMemorySameTxBehavior IN_MEMORY_SAME_TX_BEHAVIOR = new InMemorySameTxBehavior();
    private static final InMemoryNoTxBehavior IN_MEMORY_NO_TX_BEHAVIOR = new InMemoryNoTxBehavior();
    private static final InMemoryDifferentTxBehavior IN_MEMORY_DIFFERENT_TX_BEHAVIOR = new InMemoryDifferentTxBehavior();
    private static final InMemoryThreadNoTxObjectTxBehavior IN_MEMORY_THREAD_NO_TX_OBJECT_TX_BEHAVIOR = new InMemoryThreadNoTxObjectTxBehavior();
    private static final InMemoryTxEnrollBehavior IN_MEMORY_TX_ENROLL_BEHAVIOR = new InMemoryTxEnrollBehavior();

    private static final DetachedSameTxBehavior DETACHED_SAME_TX_BEHAVIOR = new DetachedSameTxBehavior();
    private static final DetachedNoTxBehavior DETACHED_NO_TX_BEHAVIOR = new DetachedNoTxBehavior();
    private static final DetachedTxEnrollBehavior DETACHED_TX_ENROLL_BEHAVIOR = new DetachedTxEnrollBehavior();

    private static final DeletedBehavior DEFAULT_DELETED_BEHAVIOR = new DeletedBehavior();
    private static final DeletedDifferentTxBehavior DELETED_DIFFERENT_TX_BEHAVIOR = new DeletedDifferentTxBehavior();
    private static final DeletedThreadNoTxObjectTxBehavior DELETED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR = new DeletedThreadNoTxObjectTxBehavior();

    private static final DetachedDeletedSameTxBehavior DETACHED_DELETED_SAME_TX_BEHAVIOR = new DetachedDeletedSameTxBehavior();
    private static final DetachedDeletedNoTxBehavior DETACHED_DELETED_NO_TX_BEHAVIOR = new DetachedDeletedNoTxBehavior();
    private static final DetachedDeletedDifferentTxBehavior DETACHED_DELETED_DIFFERENT_TX_BEHAVIOR = new DetachedDeletedDifferentTxBehavior();
    private static final DetachedDeletedThreadNoTxObjectTxBehavior DETACHED_DELETED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR = new DetachedDeletedThreadNoTxObjectTxBehavior();
    private static final DetachedDeletedTxEnrollBehavior DETACHED_DELETED_TX_ENROLL_BEHAVIOR = new DetachedDeletedTxEnrollBehavior();
    private static final InMemoryNoTxNotPersistentBehavior IN_MEMORY_NO_TX_NOT_PERSISTENT_BEHAVIOR = new InMemoryNoTxNotPersistentBehavior();

    public static PersistedSameTxBehavior getPersistedSameTxBehavior()
    {
        return PERSISTED_SAME_TX_BEHAVIOR;
    }

    public static InMemoryNoTxNotPersistentBehavior getReadOnlyInMemoryNonTxBehavior()
    {
        return IN_MEMORY_NO_TX_NOT_PERSISTENT_BEHAVIOR;
    }

    public static PersistedNonTransactionalBehavior getPersistedNonTransactionalBehavior()
    {
        return PERSISTED_NON_TRANSACTIONAL_BEHAVIOR;
    }

    public static PersistedNoTxBehavior getPersistedNoTxBehavior()
    {
        return PERSISTED_NO_TX_BEHAVIOR;
    }

    public static PersistedDifferentTxBehavior getPersistedDifferentTxBehavior()
    {
        return PERSISTED_DIFFERENT_TX_BEHAVIOR;
    }

    public static PersistedThreadNoTxObjectTxBehavior getPersistedThreadNoTxObjectTxBehavior()
    {
        return PERSISTED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR;
    }

    public static PersistedTxEnrollBehavior getPersistedTxEnrollBehavior()
    {
        return PERSISTED_TX_ENROLL_BEHAVIOR;
    }

    public static InMemorySameTxBehavior getInMemorySameTxBehavior()
    {
        return IN_MEMORY_SAME_TX_BEHAVIOR;
    }

    public static InMemoryNoTxBehavior getInMemoryNoTxBehavior()
    {
        return IN_MEMORY_NO_TX_BEHAVIOR;
    }

    public static InMemoryDifferentTxBehavior getInMemoryDifferentTxBehavior()
    {
        return IN_MEMORY_DIFFERENT_TX_BEHAVIOR;
    }

    public static InMemoryThreadNoTxObjectTxBehavior getInMemoryThreadNoTxObjectTxBehavior()
    {
        return IN_MEMORY_THREAD_NO_TX_OBJECT_TX_BEHAVIOR;
    }

    public static InMemoryTxEnrollBehavior getInMemoryTxEnrollBehavior()
    {
        return IN_MEMORY_TX_ENROLL_BEHAVIOR;
    }

    public static DetachedSameTxBehavior getDetachedSameTxBehavior()
    {
        return DETACHED_SAME_TX_BEHAVIOR;
    }

    public static DetachedNoTxBehavior getDetachedNoTxBehavior()
    {
        return DETACHED_NO_TX_BEHAVIOR;
    }

    public static DetachedTxEnrollBehavior getDetachedTxEnrollBehavior()
    {
        return DETACHED_TX_ENROLL_BEHAVIOR;
    }

    public static DeletedBehavior getDefaultDeletedBehavior()
    {
        return DEFAULT_DELETED_BEHAVIOR;
    }

    public static DeletedDifferentTxBehavior getDeletedDifferentTxBehavior()
    {
        return DELETED_DIFFERENT_TX_BEHAVIOR;
    }

    public static DeletedThreadNoTxObjectTxBehavior getDeletedThreadNoTxObjectTxBehavior()
    {
        return DELETED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR;
    }

    public static DetachedDeletedSameTxBehavior getDetachedDeletedSameTxBehavior()
    {
        return DETACHED_DELETED_SAME_TX_BEHAVIOR;
    }

    public static DetachedDeletedNoTxBehavior getDetachedDeletedNoTxBehavior()
    {
        return DETACHED_DELETED_NO_TX_BEHAVIOR;
    }

    public static DetachedDeletedDifferentTxBehavior getDetachedDeletedDifferentTxBehavior()
    {
        return DETACHED_DELETED_DIFFERENT_TX_BEHAVIOR;
    }

    public static DetachedDeletedThreadNoTxObjectTxBehavior getDetachedDeletedThreadNoTxObjectTxBehavior()
    {
        return DETACHED_DELETED_THREAD_NO_TX_OBJECT_TX_BEHAVIOR;
    }

    public static DetachedDeletedTxEnrollBehavior getDetachedDeletedTxEnrollBehavior()
    {
        return DETACHED_DELETED_TX_ENROLL_BEHAVIOR;
    }

}
