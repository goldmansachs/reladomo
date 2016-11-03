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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.FullyCachedTinyBalance;
import com.gs.fw.common.mithra.test.domain.SpecialAccount;
import com.gs.fw.common.mithra.test.domain.TestReplicatedObject;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsAccountIncomeFunction;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsAccountIncomeFunctionFinder;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrial;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrialFinder;

import java.sql.*;
import java.text.SimpleDateFormat;



public class TestMithraReplicationNotificationMessages  extends RemoteMithraReplicationNotificationTestCase
{


    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String UPD_QUEUE_INSERT = "INSERT INTO APP.AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid, rs_origin_xact_id) VALUES ";
    private static final String AP_TAMS_TRIAL_INSERT = "INSERT INTO APP.AP_TEST_TAMS_TRIAL (event_seq_no,last_update_time, NODE_I, THRU_Z, OUT_Z, action,last_update_userid ) VALUES ";
    private static final String TAMS_TRIAL_INSERT = "INSERT INTO APP.TEST_TAMS_TRIAL (node_i, trial_auto_c, from_z, thru_z, in_z, out_z) values ";
    private static final String TAMS_TRIAL_DELETE = "DELETE FROM APP.TEST_TAMS_TRIAL WHERE ";
    private static final String AP_TEST_TAMS_ACCT_IF_INSERT = "INSERT INTO APP.AP_TEST_TAMS_ACCT_IF (event_seq_no,last_update_time, ACCT_C, THRU_Z, OUT_Z, action,last_update_userid ) VALUES ";
    private static final String TEST_TAMS_ACCT_IF_INSERT = "INSERT INTO APP.TEST_TAMS_ACCT_IF (ACCT_C, PERCENTAGE_F, FROM_Z, THRU_Z, IN_Z, OUT_Z) values ";
    private static final String TEST_TAMS_ACCT_IF_DELETE = "DELETE FROM APP.TEST_TAMS_ACCT_IF WHERE ";
    protected Class[] getRestrictedClassList()
    {
        return new Class[]
        {
           TestTamsMithraTrial.class,
           TestTamsAccountIncomeFunction.class,
           TestReplicatedObject.class,
           FullyCachedTinyBalance.class,
            SpecialAccount.class
        };
    }


    public void testUpdateNotificationOnReplicatedDatedTable()
    throws Exception
    {
        int updateClassCount = TestTamsMithraTrialFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00.0").getTime());
        Operation op = TestTamsMithraTrialFinder.trialNodeId().eq(1);
        op = op.and(TestTamsMithraTrialFinder.businessDate().eq(businessDate));
        TestTamsMithraTrial trial = TestTamsMithraTrialFinder.findOne(op);
        assertEquals("1", trial.getTrialId());
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverInsertUpdateNotifications", new Class[]{},
                new Object[]{});
        waitForMessages(updateClassCount, TestTamsMithraTrialFinder.getMithraObjectPortal());
        waitForMessages(updateClassCount+1, TestTamsMithraTrialFinder.getMithraObjectPortal());
        TestTamsMithraTrial trial2 = TestTamsMithraTrialFinder.findOne(op);
        assertNotNull(trial2);
        assertEquals("2", trial2.getTrialId());
//        this.getRemoteSlaveVm().executeMethod("serverVerifyTablesAreEmpty", new Class[]{String.class},
//                new Object[]{TestTamsMithraTrialFinder.getMithraObjectPortal().getDatabaseObject().getTableName()});
    }

    public void testUpdateNotificationOnReplicatedDatedTable2()
    throws Exception
    {
        int updateClassCount = TestTamsMithraTrialFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00.0").getTime());
        Operation op = TestTamsMithraTrialFinder.trialNodeId().eq(1);
        op = op.and(TestTamsMithraTrialFinder.businessDate().eq(businessDate));
        TestTamsMithraTrial trial = TestTamsMithraTrialFinder.findOne(op);
        assertEquals("1", trial.getTrialId());
        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverInsertUpdateNotifications", new Class[]{},
                new Object[]{});
        waitForMessages(updateClassCount, TestTamsMithraTrialFinder.getMithraObjectPortal());
        waitForMessages(updateClassCount+1, TestTamsMithraTrialFinder.getMithraObjectPortal());

        assertEquals("2", trial.getTrialId());
    }

    public void testMultipleInsertDeleteNotificationOnReplicatedDatedTable()
    throws Exception
    {
        int updateClassCount = TestTamsMithraTrialFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int updateClassCount2 = TestTamsAccountIncomeFunctionFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00.0").getTime());
        Operation op = TestTamsMithraTrialFinder.trialNodeId().eq(1);
        op = op.and(TestTamsMithraTrialFinder.businessDate().eq(businessDate));

        Operation op2 = TestTamsAccountIncomeFunctionFinder.accountId().eq("1234");
        op2 = op2.and(TestTamsAccountIncomeFunctionFinder.businessDate().eq(businessDate));

        TestTamsMithraTrial trial = TestTamsMithraTrialFinder.findOne(op);
        assertEquals("1", trial.getTrialId());

        TestTamsAccountIncomeFunction incFunc = TestTamsAccountIncomeFunctionFinder.findOne(op2);
               assertEquals(25.5f, incFunc.getPercentage(), 0);

        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverInsertMultipleUpdateNotifications", new Class[]{},
                new Object[]{});
        waitForMessages(updateClassCount, TestTamsMithraTrialFinder.getMithraObjectPortal());
        waitForMessages(updateClassCount+1, TestTamsMithraTrialFinder.getMithraObjectPortal());
        waitForMessages(updateClassCount2, TestTamsAccountIncomeFunctionFinder.getMithraObjectPortal());
        waitForMessages(updateClassCount2+1, TestTamsAccountIncomeFunctionFinder.getMithraObjectPortal());
        TestTamsMithraTrial trial2 = TestTamsMithraTrialFinder.findOne(op);
        assertNotNull(trial2);
        assertEquals("2", trial2.getTrialId());
        TestTamsAccountIncomeFunction incFunc2 = TestTamsAccountIncomeFunctionFinder.findOne(op2);
        assertNotNull(incFunc2);
        assertEquals(35.5f, incFunc2.getPercentage(), 0);

    }

    public void testMultipleInsertDeleteNotificationOnReplicatedDatedTable2()
    throws Exception
    {
        int updateClassCount = TestTamsMithraTrialFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        int updateClassCount2 = TestTamsAccountIncomeFunctionFinder.getMithraObjectPortal().getPerClassUpdateCountHolder().getUpdateCount();
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00.0").getTime());
        Operation op = TestTamsMithraTrialFinder.trialNodeId().eq(1);
        op = op.and(TestTamsMithraTrialFinder.businessDate().eq(businessDate));

        Operation op2 = TestTamsAccountIncomeFunctionFinder.accountId().eq("1234");
        op2 = op2.and(TestTamsAccountIncomeFunctionFinder.businessDate().eq(businessDate));

        Operation op3 = TestTamsAccountIncomeFunctionFinder.accountId().eq("1235");
        op3 = op3.and(TestTamsAccountIncomeFunctionFinder.businessDate().eq(businessDate));

        TestTamsMithraTrial trial = TestTamsMithraTrialFinder.findOne(op);
        assertEquals("1", trial.getTrialId());

        TestTamsAccountIncomeFunction incFunc = TestTamsAccountIncomeFunctionFinder.findOne(op2);
               assertEquals(25.5f, incFunc.getPercentage(), 0);

        TestTamsAccountIncomeFunction incFunc2 = TestTamsAccountIncomeFunctionFinder.findOne(op3);
               assertEquals(50.5f, incFunc2.getPercentage(), 0);

        waitForRegistrationToComplete();
        this.getRemoteSlaveVm().executeMethod("serverInsertMultipleUpdateNotifications2", new Class[]{},
                new Object[]{});
        waitForMessages(updateClassCount, TestTamsMithraTrialFinder.getMithraObjectPortal());
        waitForMessages(updateClassCount+1, TestTamsMithraTrialFinder.getMithraObjectPortal());
        waitForMessages(updateClassCount2, TestTamsAccountIncomeFunctionFinder.getMithraObjectPortal());
        waitForMessages(updateClassCount2+1, TestTamsAccountIncomeFunctionFinder.getMithraObjectPortal());
        waitForMessages(updateClassCount2+2, TestTamsAccountIncomeFunctionFinder.getMithraObjectPortal());

        TestTamsMithraTrial trial2 = TestTamsMithraTrialFinder.findOne(op);
        assertNotNull(trial2);
        assertEquals("2", trial2.getTrialId());

        TestTamsAccountIncomeFunction incFunc3 = TestTamsAccountIncomeFunctionFinder.findOne(op2);
        assertNotNull(incFunc3);
               assertEquals(35.5f, incFunc3.getPercentage(), 0);

         TestTamsAccountIncomeFunction incFunc4 = TestTamsAccountIncomeFunctionFinder.findOne(op3);
        assertNotNull(incFunc4);
               assertEquals(60.5f, incFunc4.getPercentage(), 0);

    }


    public void serverInsertActiveReplicatedObject()
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        String insertReplicatedObjSql = "INSERT INTO TEST_REPLICATED_OBJECT (objectid, userid, name, active) VALUES(999, 'doej', 'John Doe', true)";
        String insertMasterQueueData = "INSERT INTO AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid) VALUES (1,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra')";
        String insertChildQueueData1 = "INSERT INTO AP_TEST_REPLICATED_OBJECT (event_seq_no,last_update_time, OBJECTID, action,last_update_userid ) VALUES (1,'"+now.toString()+"',999,'I','gonzra')";
        replicateSingleRow(insertReplicatedObjSql, insertMasterQueueData, insertChildQueueData1);
    }

    public void serverInsertMultipleActiveReplicatedObject()
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        String insertReplicatedObjSql = "INSERT INTO TEST_REPLICATED_OBJECT (objectid, userid, name, active) VALUES(1000, 'doej', 'John Doe', true),(1001, 'mans', 'Spider Man', true),(1002, 'smithj', 'John Smith', true),(1003, 'rezaem', 'Moh', true)";
        String insertMasterQueueData = "INSERT INTO AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid) VALUES (996,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra'),(997,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra'),(998,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra'),(999,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra')";
        String insertChildQueueData1 = "INSERT INTO AP_TEST_REPLICATED_OBJECT (event_seq_no,last_update_time, OBJECTID, action,last_update_userid ) VALUES (996,'"+now.toString()+"',1000,'I','gonzra'),(997,'"+now.toString()+"',1001,'I','gonzra'),(998,'"+now.toString()+"',1002,'I','gonzra'),(999,'"+now.toString()+"',1003,'I','gonzra')";
        replicateSingleRow(insertReplicatedObjSql, insertMasterQueueData, insertChildQueueData1);
    }

    public void serverInsertMultipleActiveReplicatedObject(int initialId, int count)
    {
        for(int i = 0; i < count; i++)
        {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            String insertReplicatedObjSql = "INSERT INTO TEST_REPLICATED_OBJECT (objectid, userid, name, active) VALUES("+(initialId+ i)+", 'doej', 'John Doe', true)";
            String insertMasterQueueData = "INSERT INTO AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid) VALUES ("+(initialId + i)+",'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra')";
            String insertChildQueueData1 = "INSERT INTO AP_TEST_REPLICATED_OBJECT (event_seq_no,last_update_time, OBJECTID, action,last_update_userid ) VALUES ("+(initialId + i)+",'"+now.toString()+"',"+(initialId + i)+",'I','gonzra')";
            replicateSingleRow(insertReplicatedObjSql, insertMasterQueueData, insertChildQueueData1);
            System.out.println("Inserted: "+i);
        }
    }

    public void serverUpdateReplicatedObjectStatus(long objectId, boolean newStatus)
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        String updateReplicatedObjSql = "update TEST_REPLICATED_OBJECT set ACTIVE = "+newStatus+" where OBJECTID = "+objectId;
        String insertMasterQueueData = "INSERT INTO AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid) VALUES (1,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra')";
        String insertChildQueueData1 = "INSERT INTO AP_TEST_REPLICATED_OBJECT (event_seq_no,last_update_time, OBJECTID, action,last_update_userid ) VALUES (1,'"+now.toString()+"',"+objectId+",'U','gonzra')";
        replicateSingleRow(updateReplicatedObjSql, insertMasterQueueData, insertChildQueueData1);
    }

    public void serverUpdateMultipleReplicatedObject()
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        String updateReplicatedObjSql = "update TEST_REPLICATED_OBJECT set ACTIVE = true where ACTIVE = false ";
        String insertMasterQueueData = "INSERT INTO AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid) VALUES (996,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra'),(997,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra')";
        String insertChildQueueData1 = "INSERT INTO AP_TEST_REPLICATED_OBJECT (event_seq_no,last_update_time, OBJECTID, action,last_update_userid ) VALUES (996,'"+now.toString()+"',2,'U','gonzra'), (997,'"+now.toString()+"',3,'U','gonzra')";
        replicateSingleRow(updateReplicatedObjSql, insertMasterQueueData, insertChildQueueData1);
    }

    public void serverDeleteReplicatedObject(long objectId)

    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        String updateReplicatedObjSql = "delete from TEST_REPLICATED_OBJECT where OBJECTID = "+objectId;
        String insertMasterQueueData = "INSERT INTO AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid) VALUES (1,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra')";
        String insertChildQueueData1 = "INSERT INTO AP_TEST_REPLICATED_OBJECT (event_seq_no,last_update_time, OBJECTID, action,last_update_userid ) VALUES (1,'"+now.toString()+"',"+objectId+",'D','gonzra')";
        replicateSingleRow(updateReplicatedObjSql, insertMasterQueueData, insertChildQueueData1);
    }

    public void serverDeleteMultipleReplicatedObject()
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        String updateReplicatedObjSql = "delete from TEST_REPLICATED_OBJECT where ACTIVE = false";
        String insertMasterQueueData = "INSERT INTO AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid) VALUES (1,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra'),(2,'"+now.toString()+"','TEST_REPLICATED_OBJECT','gonzra')";
        String insertChildQueueData1 = "INSERT INTO AP_TEST_REPLICATED_OBJECT (event_seq_no,last_update_time, OBJECTID, action,last_update_userid ) VALUES (1,'"+now.toString()+"',2,'D','gonzra'), (1,'"+now.toString()+"',3,'D','gonzra')";
        replicateSingleRow(updateReplicatedObjSql, insertMasterQueueData, insertChildQueueData1);
    }

    public void serverUpdateTamsTrial()
    throws Exception
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Timestamp infinity = new Timestamp(timestampFormat.parse("9999-12-01 23:59:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2000-01-01 00:00:00.0").getTime());
        String chainTrial1 = "update APP.TEST_TAMS_TRIAL set THRU_Z = '"+now.toString()+"', OUT_Z = '"+now.toString()+"' WHERE NODE_I = 1 AND THRU_Z = '"+infinity.toString()+"' and OUT_Z = '"+infinity.toString()+"'";
        String chainTrial2 = "insert into APP.TEST_TAMS_TRIAL (node_i, trial_auto_c, from_z, thru_z, in_z, out_z) values (1, '2', '"+businessDate.toString()+"', '"+infinity.toString()+"', '"+now.toString()+"', '"+infinity.toString()+"')";
        String insertMasterQueueData1 = "INSERT INTO APP.AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid) VALUES (1, '"+now.toString()+"', 'TEST_TAMS_TRIAL', 'gonzra')";
        String insertMasterQueueData2 = "INSERT INTO APP.AP_UPD_QUEUE (event_seq_no, last_update_time, entity_n, last_update_userid) VALUES (2, '"+now.toString()+"', 'TEST_TAMS_TRIAL', 'gonzra')";
        String insertChildQueueData1 = "INSERT INTO APP.AP_TEST_TAMS_TRIAL (event_seq_no,last_update_time, NODE_I, THRU_Z, OUT_Z, action,last_update_userid ) VALUES (1,'"+now.toString()+"',1,'"+now.toString()+"','"+now.toString()+"','U','gonzra')";
        String insertChildQueueData2 = "INSERT INTO APP.AP_TEST_TAMS_TRIAL (event_seq_no,last_update_time, NODE_I, THRU_Z, OUT_Z, action,last_update_userid ) VALUES (2,'"+now.toString()+"',1,'"+infinity.toString()+"','"+infinity.toString()+"','I','gonzra')";
        replicateSingleRow(chainTrial1, insertMasterQueueData1, insertChildQueueData1);
        replicateSingleRow(chainTrial2, insertMasterQueueData2, insertChildQueueData2);
    }



    public void serverInsertUpdateNotifications()
    throws Exception
    {
        MithraManagerProvider.getMithraManager().getReplicationNotificationManager().setBatchSize(5);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Timestamp infinity = new Timestamp(timestampFormat.parse("9999-12-01 23:59:00.0").getTime());
        Timestamp originalDate = new Timestamp(timestampFormat.parse("2000-01-01 00:00:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00.0").getTime());
        String chainTrial1 = TAMS_TRIAL_DELETE + createTamsTrialDeleteValuesClause(1,infinity.toString(), infinity.toString());
        String chainTrial2 = TAMS_TRIAL_INSERT +  createTamsTrialInsertValuesClause(1, "1", originalDate.toString(), infinity.toString(),originalDate.toString(), now.toString());
        String chainTrial3 = TAMS_TRIAL_INSERT + createTamsTrialInsertValuesClause(1, "1", originalDate.toString(), businessDate.toString(), now.toString(), infinity.toString());
        String chainTrial4 = TAMS_TRIAL_INSERT + createTamsTrialInsertValuesClause(1, "2", businessDate.toString(), infinity.toString(), now.toString(), infinity.toString());
        int start = (int)(Math.random() * 1000000); // fix for non-reproducible unit test failure
        String insertMasterQueueData1 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(start + 1, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData2 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(start + 2, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData3 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(start + 3, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData4 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(start + 4, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");

        String insertChildQueueData1 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(start + 1, now.toString(), 1, infinity.toString(), infinity.toString(), "D", "gonzra");
        String insertChildQueueData2 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(start + 2, now.toString(), 1, infinity.toString(), now.toString() ,"I", "gonzra");
        String insertChildQueueData3 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(start + 3, now.toString(), 1, businessDate.toString(), infinity.toString(), "I", "gonzra");
        String insertChildQueueData4 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(start + 4, now.toString(), 1, infinity.toString(), infinity.toString(), "I", "gonzra");

        replicateSingleRow(chainTrial1, insertMasterQueueData1, insertChildQueueData1);
        replicateSingleRow(chainTrial2, insertMasterQueueData2, insertChildQueueData2);
        replicateSingleRow(chainTrial3, insertMasterQueueData3, insertChildQueueData3);
        replicateSingleRow(chainTrial4, insertMasterQueueData4, insertChildQueueData4);

    }

    public void serverInsertMultipleUpdateNotifications()
    throws Exception
    {
        MithraManagerProvider.getMithraManager().getReplicationNotificationManager().setBatchSize(5);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Timestamp infinity = new Timestamp(timestampFormat.parse("9999-12-01 23:59:00.0").getTime());
        Timestamp originalDate = new Timestamp(timestampFormat.parse("2000-01-01 00:00:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00.0").getTime());

        //Update TamsTrial
        String chainTrial1 = TAMS_TRIAL_DELETE + createTamsTrialDeleteValuesClause(1,infinity.toString(), infinity.toString());
        String chainTrial2 = TAMS_TRIAL_INSERT +  createTamsTrialInsertValuesClause(1, "1", originalDate.toString(), infinity.toString(),originalDate.toString(), now.toString());
        String chainTrial3 = TAMS_TRIAL_INSERT + createTamsTrialInsertValuesClause(1, "1", originalDate.toString(), businessDate.toString(), now.toString(), infinity.toString());
        String chainTrial4 = TAMS_TRIAL_INSERT + createTamsTrialInsertValuesClause(1, "2", businessDate.toString(), infinity.toString(), now.toString(), infinity.toString());
        String insertMasterQueueData1 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(1, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData2 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(2, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData3 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(3, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData4 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(4, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");

        String insertChildQueueData1 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(1, now.toString(), 1, infinity.toString(), infinity.toString(), "D", "gonzra");
        String insertChildQueueData2 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(2, now.toString(), 1, infinity.toString(), now.toString() ,"I", "gonzra");
        String insertChildQueueData3 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(3, now.toString(), 1, businessDate.toString(), infinity.toString(), "I", "gonzra");
        String insertChildQueueData4 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(4, now.toString(), 1, infinity.toString(), infinity.toString(), "I", "gonzra");

        replicateSingleRow(chainTrial1, insertMasterQueueData1, insertChildQueueData1);
        replicateSingleRow(chainTrial2, insertMasterQueueData2, insertChildQueueData2);
        replicateSingleRow(chainTrial3, insertMasterQueueData3, insertChildQueueData3);
        replicateSingleRow(chainTrial4, insertMasterQueueData4, insertChildQueueData4);

        //Insert TamsIF
        String insertSql1 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("9876", 25.5f, now.toString(), infinity.toString(), now.toString(), infinity.toString());
        String insertMasterQueueData5 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(5, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertChildQueueData5 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(5, now.toString(), "1235", infinity.toString(), infinity.toString(), "I", "gonzra");
        replicateSingleRow(insertSql1, insertMasterQueueData5, insertChildQueueData5);

        //Update TamsIF
        String deletrIF1 = TEST_TAMS_ACCT_IF_DELETE + createTestTamsAcctIfDeleteValuesClause("1234", infinity.toString(), infinity.toString());
        String insertIF1 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("1234", 25.5f, originalDate.toString(), infinity.toString(), originalDate.toString(), now.toString());
        String insertIF2 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("1234", 25.5f, originalDate.toString(), businessDate.toString(), now.toString(), infinity.toString());
        String insertIF3 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("1234", 35.5f, businessDate.toString(), infinity.toString(), now.toString(), infinity.toString());

        String insertMasterQueueData6 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(6, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "10010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData7 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(7, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "10010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData8 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(8, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "10010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData9 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(9, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "10010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");


        String insertChildQueueData6 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(6, now.toString(), "1234", infinity.toString(), infinity.toString(), "D", "gonzra");
        String insertChildQueueData7 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(7, now.toString(), "1234", infinity.toString(), now.toString(), "I", "gonzra");
        String insertChildQueueData8 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(8, now.toString(), "1234", businessDate.toString(), infinity.toString(), "I", "gonzra");
        String insertChildQueueData9 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(9, now.toString(), "1234", infinity.toString(), infinity.toString(), "I", "gonzra");


        replicateSingleRow(deletrIF1, insertMasterQueueData6, insertChildQueueData6);
        replicateSingleRow(insertIF1, insertMasterQueueData7, insertChildQueueData7);
        replicateSingleRow(insertIF2, insertMasterQueueData8, insertChildQueueData8);
        replicateSingleRow(insertIF3, insertMasterQueueData9, insertChildQueueData9);


    }

    public void serverInsertMultipleUpdateNotifications2()
    throws Exception
    {
        MithraManagerProvider.getMithraManager().getReplicationNotificationManager().setBatchSize(5);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Timestamp infinity = new Timestamp(timestampFormat.parse("9999-12-01 23:59:00.0").getTime());
        Timestamp originalDate = new Timestamp(timestampFormat.parse("2000-01-01 00:00:00.0").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2002-01-01 00:00:00.0").getTime());

        //Update TamsTrial
        String chainTrial1 = TAMS_TRIAL_DELETE + createTamsTrialDeleteValuesClause(1,infinity.toString(), infinity.toString());
        String chainTrial2 = TAMS_TRIAL_INSERT +  createTamsTrialInsertValuesClause(1, "1", originalDate.toString(), infinity.toString(),originalDate.toString(), now.toString());
        String chainTrial3 = TAMS_TRIAL_INSERT + createTamsTrialInsertValuesClause(1, "1", originalDate.toString(), businessDate.toString(), now.toString(), infinity.toString());
        String chainTrial4 = TAMS_TRIAL_INSERT + createTamsTrialInsertValuesClause(1, "2", businessDate.toString(), infinity.toString(), now.toString(), infinity.toString());
        String insertMasterQueueData1 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(1, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData2 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(2, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData3 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(3, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData4 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(4, now.toString(), "TEST_TAMS_TRIAL", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");

        String insertChildQueueData1 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(1, now.toString(), 1, infinity.toString(), infinity.toString(), "D", "gonzra");
        String insertChildQueueData2 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(2, now.toString(), 1, infinity.toString(), now.toString() ,"I", "gonzra");
        String insertChildQueueData3 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(3, now.toString(), 1, businessDate.toString(), infinity.toString(), "I", "gonzra");
        String insertChildQueueData4 = AP_TAMS_TRIAL_INSERT + createApTamsTrialValuesClause(4, now.toString(), 1, infinity.toString(), infinity.toString(), "I", "gonzra");

        replicateSingleRow(chainTrial1, insertMasterQueueData1, insertChildQueueData1);
        replicateSingleRow(chainTrial2, insertMasterQueueData2, insertChildQueueData2);
        replicateSingleRow(chainTrial3, insertMasterQueueData3, insertChildQueueData3);
        replicateSingleRow(chainTrial4, insertMasterQueueData4, insertChildQueueData4);

        //Update TamsIF
        String deletrIF1 = TEST_TAMS_ACCT_IF_DELETE + createTestTamsAcctIfDeleteValuesClause("1234", infinity.toString(), infinity.toString());
        String insertIF1 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("1234", 25.5f, originalDate.toString(), infinity.toString(), originalDate.toString(), now.toString());
        String insertIF2 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("1234", 25.5f, originalDate.toString(), businessDate.toString(), now.toString(), infinity.toString());
        String insertIF3 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("1234", 35.5f, businessDate.toString(), infinity.toString(), now.toString(), infinity.toString());

        String insertMasterQueueData5 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(5, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData6 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(6, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData7 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(7, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData8 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(8, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "00010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");


        String insertChildQueueData5 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(5, now.toString(), "1234", infinity.toString(), infinity.toString(), "D", "gonzra");
        String insertChildQueueData6 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(6, now.toString(), "1234", infinity.toString(), now.toString(), "I", "gonzra");
        String insertChildQueueData7 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(7, now.toString(), "1234", businessDate.toString(), infinity.toString(), "I", "gonzra");
        String insertChildQueueData8 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(8, now.toString(), "1234", infinity.toString(), infinity.toString(), "I", "gonzra");


        replicateSingleRow(deletrIF1, insertMasterQueueData5, insertChildQueueData5);
        replicateSingleRow(insertIF1, insertMasterQueueData6, insertChildQueueData6);
        replicateSingleRow(insertIF2, insertMasterQueueData7, insertChildQueueData7);
        replicateSingleRow(insertIF3, insertMasterQueueData8, insertChildQueueData8);


        //Update TamsIF
        now = new Timestamp(System.currentTimeMillis());
        String deletrIF2 = TEST_TAMS_ACCT_IF_DELETE + createTestTamsAcctIfDeleteValuesClause("1235", infinity.toString(), infinity.toString());
        String insertIF4 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("1235", 50.5f, originalDate.toString(), infinity.toString(), originalDate.toString(), now.toString());
        String insertIF5 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("1235", 50.5f, originalDate.toString(), businessDate.toString(), now.toString(), infinity.toString());
        String insertIF6 = TEST_TAMS_ACCT_IF_INSERT + createTestTamsAcctIfInsertValuesClause("1235", 60.5f, businessDate.toString(), infinity.toString(), now.toString(), infinity.toString());

        String insertMasterQueueData9 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(9, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "10010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData10 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(10, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "10010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData11 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(11, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "10010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        String insertMasterQueueData12 = UPD_QUEUE_INSERT + createUpdQueueValuesClause(12, now.toString(), "TEST_TAMS_ACCT_IF", "gonzra", "10010001b5bfb5ba00174e5954414d5350316e74616d735f70726f640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");


        String insertChildQueueData9 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(9, now.toString(), "1235", infinity.toString(), infinity.toString(), "D", "gonzra");
        String insertChildQueueData10 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(10, now.toString(), "1235", infinity.toString(), now.toString(), "I", "gonzra");
        String insertChildQueueData11 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(11, now.toString(), "1235", businessDate.toString(), infinity.toString(), "I", "gonzra");
        String insertChildQueueData12 = AP_TEST_TAMS_ACCT_IF_INSERT + createApTestTamsAcctIfValuesClause(12, now.toString(), "1235", infinity.toString(), infinity.toString(), "I", "gonzra");


        replicateSingleRow(deletrIF2, insertMasterQueueData9, insertChildQueueData9);
        replicateSingleRow(insertIF4, insertMasterQueueData10, insertChildQueueData10);
        replicateSingleRow(insertIF5, insertMasterQueueData11, insertChildQueueData11);
        replicateSingleRow(insertIF6, insertMasterQueueData12, insertChildQueueData12);
    }

    private String createUpdQueueValuesClause(int id, String timestamp, String entity, String user, String txid)
    {
        return "("+id +", '"+timestamp+"', '"+entity+"', '"+user+"', X'"+txid+"')";
    }

    private String createApTamsTrialValuesClause(int id, String timestamp, long nodeI, String thruz, String outz, String action, String user)
    {
        return "("+id +", '"+timestamp+"', "+nodeI+", '"+thruz+"', '"+outz+"', '"+action+"', '"+user+"')";
    }

    private String createTamsTrialInsertValuesClause(long node_i, String trialCode, String fromz, String thruz, String inz, String outz)
    {
         return "("+node_i+", '"+trialCode+"', '"+fromz+"' , '"+thruz+"', '"+inz+"' , '"+outz+"')";
    }

    private String createTamsTrialDeleteValuesClause(long nodeI, String thruz, String outz)
    {
        return " NODE_I = "+nodeI+" AND THRU_Z = '"+thruz+"' AND OUT_Z = '"+outz+"'";

    }

    private String createApTestTamsAcctIfValuesClause(int id, String timestamp, String acctC, String thruz, String outz, String action, String user)
    {
        return "("+id +", '"+timestamp+"', '"+acctC+"', '"+thruz+"', '"+outz+"', '"+action+"', '"+user+"')";
    }

    private String createTestTamsAcctIfInsertValuesClause(String acctC, float percentage, String fromz, String thruz, String inz, String outz)
    {
         return "('"+acctC+"', "+percentage+", '"+fromz+"' , '"+thruz+"', '"+inz+"' , '"+outz+"')";
    }

    private String createTestTamsAcctIfDeleteValuesClause(String acctC, String thruz, String outz)
    {
        return " ACCT_C = '"+acctC+"' AND THRU_Z = '"+thruz+"' AND OUT_Z = '"+outz+"'";
    }

    private void replicateSingleRow(String replicatedObjSql, String insertMasterQueueData, String insertChildQueueData1)
    {
        Connection con = ConnectionManagerForTests.getInstance().getConnection();
        PreparedStatement replicatedObjPs = null;
        PreparedStatement masterQueuePs = null;
        PreparedStatement childQueuePs1 = null;

        try
        {
            con.setAutoCommit(false);
            replicatedObjPs = con.prepareStatement(replicatedObjSql);
            int count = replicatedObjPs.executeUpdate();
            //assertEquals(1, count);

            masterQueuePs = con.prepareStatement(insertMasterQueueData);
            count = masterQueuePs.executeUpdate();
            //assertEquals(1, count);

            childQueuePs1 = con.prepareStatement(insertChildQueueData1);
            count = childQueuePs1.executeUpdate();
            //assertEquals(1, count);

        con.commit();
            con.setAutoCommit(true);
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
           try
           {
               if(replicatedObjPs != null)
                   replicatedObjPs.close();
               if(masterQueuePs != null)
                   masterQueuePs.close();
               if(childQueuePs1 != null)
                   childQueuePs1.close();

               if(con != null)
                   con.close();
           }
           catch(SQLException e)
           {
                throw new RuntimeException(e);
           }

        }
    }

    public void serverVerifyTablesAreEmpty(String tableName)
            throws Exception
    {
        Connection con = ConnectionManagerForTests.getInstance().getConnection();
        PreparedStatement countQuery = null;
        ResultSet rs = null;
        String sql = "select count(*) from APP.ap_"+tableName;

        try
        {
            countQuery = con.prepareStatement(sql);
            rs = countQuery.executeQuery();
            assertEquals(0, rs.getInt(1));
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
           try
           {
               if(countQuery != null)
                   countQuery.close();
               if(rs != null)
                   rs.close();
               if(con != null)
                   con.close();
           }
           catch(SQLException e)
           {
                throw new RuntimeException(e);
           }
        }
    }


}
