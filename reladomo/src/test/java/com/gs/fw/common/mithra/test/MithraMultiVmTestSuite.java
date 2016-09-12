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

import com.gs.fw.common.mithra.test.evo.TestEmbeddedValueObjectsRemote;
import com.gs.fw.common.mithra.test.multivm.MultiClientVmTestSuite;
import com.gs.fw.common.mithra.test.multivm.MultiVmNotificationsTestSuite;
import com.gs.fw.common.mithra.test.multivm.MultiVmTestSuite;
import com.gs.fw.common.mithra.test.multivm.TestMultiVmTestCase;
import junit.framework.Test;
import junit.framework.TestSuite;


public class MithraMultiVmTestSuite extends TestSuite
{

    public static Test suite()
    {
        TestSuite suite = new TestSuite();

        suite.addTest(new MultiVmTestSuite(TestMultiVmTestCase.class));
        suite.addTest(new MultiVmNotificationsTestSuite(TestNotificationMessages.class));
        suite.addTest(new MultiClientVmTestSuite(TestMultiClientNotificationTestCase.class));
//        suite.addTest(new MultiVmNotificationsTestSuite(TestPeerToPeerMithraTestCase.class));
        suite.addTest(new MultiVmNotificationsTestSuite(TestApplicationNotification.class));
        suite.addTest(new MultiVmNotificationsTestSuite(TestMithraReplicationNotificationMessages.class));
        suite.addTest(new MultiVmTestSuite(TestClientPortal.class));
        suite.addTest(new MultiVmTestSuite(TestClientPortalUsingDerby.class));
        suite.addTest(new MultiVmTestSuite(TestClientPortalTimeoutDuringDatabaseOperation.class));
        suite.addTest(new MultiVmTestSuite(TestTransactionalClientPortal.class));
        suite.addTest(new MultiVmTestSuite(TestEmbeddedValueObjectsRemote.class));
        suite.addTest(new MultiVmTestSuite(TestSerializationAcrossTimezones.class));
//        suite.addTest(new MultiVmTestSuite(TestPureObjectsRemote.class));
        return suite;

    }
}
