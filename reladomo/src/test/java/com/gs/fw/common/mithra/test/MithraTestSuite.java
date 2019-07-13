/*
 Copyright 2017 Goldman Sachs.
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

import com.gs.fw.common.mithra.portal.MithraAbstractObjectPortal;
import com.gs.fw.common.mithra.test.aggregate.AggregateTestSuite;
import com.gs.fw.common.mithra.test.database.SyslogCheckerTest;
import com.gs.fw.common.mithra.test.domain.inherited.TestIndexCreation;
import com.gs.fw.common.mithra.test.evo.TestEmbeddedValueObjects;
import com.gs.fw.common.mithra.test.finalgetter.TestFinalGetters;
import com.gs.fw.common.mithra.test.h2batch.TestH2DefaultBatchSizeTestCases;
import com.gs.fw.common.mithra.test.h2batch.TestH2LargeBatchSizeTestCases;
import com.gs.fw.common.mithra.test.h2batch.TestH2NegativeBatchSizeTestCases;
import com.gs.fw.common.mithra.test.h2batch.TestH2SmallBatchSizeTestCases;
import com.gs.fw.common.mithra.test.h2batch.TestH2ZeroBatchSizeTestCases;
import com.gs.fw.common.mithra.test.inherited.TestReadOnlyInherited;
import com.gs.fw.common.mithra.test.inherited.TestTxInherited;
import com.gs.fw.common.mithra.test.mithrainterface.TestMithraInterfaceType;
import com.gs.fw.common.mithra.test.mtloader.TestMatcherThread;
import com.gs.fw.common.mithra.test.mtloader.TestMatcherThreadCustomComparator;
import com.gs.fw.common.mithra.test.mtloader.TestNonUniqueMatcherThread;
import com.gs.fw.common.mithra.test.mtloader.TestNonUniqueMatcherThreadCustomComparator;
import com.gs.fw.common.mithra.test.overlap.OverlapFixerBusinessDateMilestonedTest;
import com.gs.fw.common.mithra.test.overlap.OverlapFixerFullyMilestonedTest;
import com.gs.fw.common.mithra.test.overlap.OverlapFixerProcessingDateMilestonedTest;
import com.gs.fw.common.mithra.test.pure.TestPureObjects;
import com.gs.fw.common.mithra.test.util.*;
import com.gs.fw.common.mithra.test.util.serializer.TestTrivialJson;
import com.gs.fw.common.mithra.util.MithraCpuBoundThreadPool;
import junit.framework.Test;
import junit.framework.TestSuite;


public class MithraTestSuite
        extends TestSuite
{

    public static Test suite()
    {
        String xmlFile = System.getProperty("mithra.xml.config");
        boolean isFullCache = xmlFile.contains("FullCache");

        MithraCpuBoundThreadPool.setParallelThreshold(2);
        MithraAbstractObjectPortal.setTransitiveThreshold(2);
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestConnectionManager.class);
        suite.addTestSuite(TestCursor.class);

        suite.addTestSuite(TestRelationshipPersistence.class);
        suite.addTestSuite(TestListMerge.class);
        suite.addTestSuite(TransactionalInsertTest.class);

        //Test primary key generators
        suite.addTestSuite(TestSimulatedSequence.class);
        suite.addTestSuite(TestMaxFromTable.class);
        suite.addTestSuite(TestGeneratedCompoundPrimaryKeys.class);
        suite.addTestSuite(TestJavaTypesInPk.class);

        //Tests basic object retrieval, literally
        suite.addTestSuite(TestBasicRetrieval.class);
        suite.addTestSuite(TestBasicRetrievalFromZippedFile.class);

        suite.addTestSuite(TestFloatDoubleNaNInsert.class);
        suite.addTestSuite(TestCopyTransactionalObjectAttributes.class);
        suite.addTestSuite(TestCheckChangedObject.class);

        //Tests each operation per sql datatype
        suite.addTestSuite(TestEq.class);
        suite.addTestSuite(TestIn.class);
        suite.addTestSuite(TestTupleTempTableCreationFailure.class);
        suite.addTestSuite(TestInOperations.class);
        suite.addTestSuite(TestInMemoryNonTransactionalObjects.class);
        suite.addTestSuite(TestGreaterThan.class);
        suite.addTestSuite(TestGreaterThanEquals.class);
        suite.addTestSuite(TestLessThan.class);
        suite.addTestSuite(TestLessThanEquals.class);
        suite.addTestSuite(TestAttributeValueSelector.class);
        suite.addTestSuite(TestCalculated.class);
        suite.addTestSuite(TestCalculatedString.class);
        suite.addTestSuite(TestNotEq.class);
        suite.addTestSuite(TestNotIn.class);
        suite.addTestSuite(TestStringLike.class);
        suite.addTestSuite(TestSelfNotEquality.class);
        suite.addTestSuite(TestFilterEquality.class);
        suite.addTestSuite(TestExists.class);
        suite.addTestSuite(TestTupleIn.class);
        suite.addTestSuite(TestFindByPrimaryKey.class);

        //Tests "and" combinations with more than 2 clauses
        suite.addTestSuite(TestComplexAnd.class);

        //Tests orderby (ASC, DESC) per sql type
        suite.addTestSuite(TestOrderby.class);
        suite.addTestSuite(TestOrderbyUsingDerby.class);

        //All null tests should go here
        suite.addTestSuite(TestNullRetrieval.class);
        suite.addTestSuite(TestNullPrimaryKeyColumn.class);

        //Test relationships that have more than one PK column
        suite.addTestSuite(TestComplexKeyRelationship.class);

        suite.addTestSuite(TestRelationships.class);
        suite.addTestSuite(TestRelationshipsNoInClause.class);
        suite.addTestSuite(TestRelationshipsWithNullableFk.class);
        suite.addTestSuite(TestDirectRefRelationships.class);
        suite.addTestSuite(TestDatedBasicRetrieval.class);
        suite.addTestSuite(SelfJoinTest.class);
        suite.addTestSuite(MultiThreadDeepFetchTest.class);
        suite.addTestSuite(TestDeepFetchExternalClose.class);
        suite.addTestSuite(EdgePointDeepFetchOnProcessingTemporalTest.class);
        suite.addTestSuite(TestQueryCacheBehavior.class);

        // test dated relationships
        suite.addTestSuite(TestToDatedRelationshipViaColumn.class);
        suite.addTestSuite(TestBasicBooleanOperation.class);
        suite.addTestSuite(TestBasicByteOperation.class);
        suite.addTestSuite(TestAdditionalRelationships.class);

        suite.addTestSuite(TestMappedOperation.class);

        // transactional tests
        suite.addTestSuite(TestBasicTransactionalRetrieval.class);
        suite.addTestSuite(TestTransactionalObject.class);
        suite.addTestSuite(TestTransactionalList.class);
        suite.addTestSuite(TestTransactionalAdhocFastList.class);
        suite.addTestSuite(TestForceRefresh.class);
        suite.addTestSuite(TestConcurrentTransactions.class);
        suite.addTestSuite(TestInheritance.class);
        suite.addTestSuite(TestComplexPKUpdate.class);
        suite.addTestSuite(TestDetached.class);
        suite.addTestSuite(TestDetachedListUsesCache.class);
        suite.addTestSuite(TestDetachedDatedListUsesCache.class);
        suite.addTestSuite(TestDetachedOptimisticAuditOnly.class);
        suite.addTestSuite(TestUpdateListener.class);
        suite.addTestSuite(TestAdhocDeepFetch.class);
        suite.addTestSuite(TestNotificationDuringDeepFetch.class);
        suite.addTestSuite(TestCrossDatabaseAdhocDeepFetch.class);
        suite.addTestSuite(TestTransactionalObjectAttributesBehavior.class);

        // test basic join between dated and non-dated tables
        suite.addTestSuite(TestDatedWithNotDatedJoin.class);
        suite.addTestSuite(TestMixedSqlTimestampJoin.class);

        // test dated transactional objects
        suite.addTestSuite(TestDatedBitemporal.class);
        suite.addTestSuite(TestDatedBitemporalNull.class);
        suite.addTestSuite(TestParaDatedBitemporal.class);
        suite.addTestSuite(TestUtcDatedBitemporal.class);
        suite.addTestSuite(TestDatedNonAudited.class);
        suite.addTestSuite(TestDatedNonAuditedNull.class);
        suite.addTestSuite(TestDatedAuditOnly.class);
        suite.addTestSuite(TestDetachedAuditOnly.class);
        suite.addTestSuite(TestDatedDetached.class);
        suite.addTestSuite(TestDatedDetachedRelationshipPersistence.class);

        suite.addTestSuite(TestTimezoneConversionNewYork.class);
        suite.addTestSuite(TestTimezoneConversionTokyo.class);

        // test relationship between nondated to dated to dated
        suite.addTestSuite(TestDatedRelationship.class);

        // test serialization (not in the context of remoting)
        suite.addTestSuite(TestNonRemoteSerialization.class);

        suite.addTestSuite(TestByteArray.class);
        suite.addTestSuite(TestBigDecimal.class);
        // test updates of various types of attributes
        suite.addTestSuite(TestAttributeUpdateWrapper.class);
        suite.addTestSuite(TestSetReadOnlyAttributes.class);

        // test various operations of different data types
        suite.addTestSuite(TestDifferentDataTypeOperations.class);

        // test various operations on mapped attributes
        suite.addTestSuite(TestMappedAttributes.class);

        suite.addTestSuite(TestVerboseSerializer.class);
        suite.addTestSuite(MithraArrayTupleTupleSetTest.class);

        suite.addTestSuite(TestReadOnlyTransactionParticipation.class);
        suite.addTestSuite(TestDatedBitemporalOptimisticLocking.class);
        suite.addTestSuite(TestOptimisticTransactionParticipation.class);

        // tests for inheritance
        suite.addTestSuite(TestReadOnlyInherited.class);
        suite.addTestSuite(TestTxInherited.class);
        suite.addTestSuite(TestIndexCreation.class);
        //tests for aggregate functions
        suite.addTest(AggregateTestSuite.suite());

        //test SQE ErrorHandler
        suite.addTestSuite(SingleQueueExecutorTest.class);

        //test retry after timeout
        suite.addTestSuite(TestH2RetryAfterTimeout.class);
        //test message thrown in MithraUniqueIndexViolationException
        suite.addTestSuite(TestUniqueIndexViolationExceptionForH2.class);

        suite.addTestSuite(TestArithmeticOperationInSearch.class);
        suite.addTestSuite(TestIdentityTable.class);

        //test embedded value objects
        suite.addTestSuite(TestEmbeddedValueObjects.class);

        // pure objects
        suite.addTestSuite(TestPureObjects.class);
        suite.addTestSuite(TestPureTransactionalObject.class);
        suite.addTestSuite(TestPureBitemporalTransactionalObject.class);

        // temp objects
        suite.addTestSuite(TestTempObject.class);

        if (!isFullCache)
        {
            //test file extraction of mithra objects
            suite.addTestSuite(DbExtractorTest.class);
            suite.addTestSuite(DbExtractorMergeTest.class);
            suite.addTestSuite(DbExtractorTransformTest.class);
            suite.addTestSuite(MithraObjectGraphExtractorTest.class);
        }

        // test SingleQueueExecutor with Non Dated mithra objects
        suite.addTestSuite(NonDatedSingleQueueExecutorTest.class);

        // test Mithra interfaces
        suite.addTestSuite(TestMithraInterfaces.class);

        // test RuntimeCacheController
        suite.addTestSuite(TestRuntimeCacheController.class);
        suite.addTestSuite(TestRuntimeCacheControllerPure.class);

        suite.addTestSuite(TestGetNonPersistentCopy.class);
        suite.addTestSuite(TestMithraAbstractDatabaseObject.class);

        suite.addTestSuite(ParametizedRelationshipTest.class);

        suite.addTestSuite(TestFinalGetters.class);

        suite.addTestSuite(TestMithraInterfaceType.class);

        suite.addTestSuite(TestOperation.class);
        suite.addTestSuite(OverlapFixerFullyMilestonedTest.class);
        suite.addTestSuite(OverlapFixerBusinessDateMilestonedTest.class);
        suite.addTestSuite(OverlapFixerProcessingDateMilestonedTest.class);

        suite.addTestSuite(TestIsNullOrLargeInOperation.class);
        suite.addTestSuite(TestPersistedNonTransactional.class);

        //time support
        suite.addTestSuite(TestTimeTransactional.class);
        suite.addTestSuite(TestTimeDatedNonTransactional.class);
        suite.addTestSuite(TestTimeDatedTransactional.class);
        suite.addTestSuite(TestTimeNonTransactional.class);
        suite.addTestSuite(TestTimeTenMillis.class);
        suite.addTestSuite(TestTimeTuple.class);

        //year, day, day of month on DateAttribute and TimeAttribute
        suite.addTestSuite(TestYearMonthDayOfMonth.class);
        suite.addTestSuite(TestYearMonthTuple.class);

        suite.addTestSuite(MithraPerformanceDataTest.class);
        suite.addTestSuite(MultiThreadedBatchProcessorTest.class);

        //MTLoader
        suite.addTestSuite(TestMatcherThread.class);
        suite.addTestSuite(TestMatcherThreadCustomComparator.class);
        suite.addTestSuite(TestNonUniqueMatcherThread.class);
        suite.addTestSuite(TestNonUniqueMatcherThreadCustomComparator.class);

        // H2 Batch Operations
        suite.addTestSuite(TestH2NegativeBatchSizeTestCases.class);
        suite.addTestSuite(TestH2ZeroBatchSizeTestCases.class);
        suite.addTestSuite(TestH2DefaultBatchSizeTestCases.class);
        suite.addTestSuite(TestH2SmallBatchSizeTestCases.class);
        suite.addTestSuite(TestH2LargeBatchSizeTestCases.class);

        // SyslogChecker
        suite.addTestSuite(SyslogCheckerTest.class);

        //Serialization
        suite.addTestSuite(TestTrivialJson.class);

        //SubQuery
        suite.addTestSuite(TestSubQueryCache.class);

        // utf-8 test charset
        suite.addTestSuite(TestUtf8TestCharset.class);

        return suite;
    }
}
