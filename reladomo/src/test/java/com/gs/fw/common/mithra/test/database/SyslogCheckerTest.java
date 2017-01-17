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

package com.gs.fw.common.mithra.test.database;

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.IntegerAttribute;
import com.gs.fw.common.mithra.database.MithraCodeGeneratedDatabaseObject;
import com.gs.fw.common.mithra.database.SyslogChecker;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.Employee;
import com.gs.fw.common.mithra.test.domain.EmployeeFinder;
import com.gs.fw.common.mithra.test.domain.SpecialAccount;
import com.gs.fw.common.mithra.test.domain.SpecialAccountFinder;
import com.gs.fw.common.mithra.test.tax.Address;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;

public class SyslogCheckerTest extends MithraTestAbstract
{
    private static final SyslogChecker CHECKER = new SyslogChecker(50.0, 5);

    public void testNoSourceAttribute()
    {
        final Address address = new Address(DefaultInfinityTimestamp.getDefaultInfinity());
        address.setAddressId(1);
        address.setChangedBy("N");
        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand<Address>()
        {
            @Override
            public Address executeTransaction(MithraTransaction tx) throws Throwable
            {
                address.insert();
                return address;
            }
        });

        CHECKER.checkAndWaitForSyslog(address);

        final MithraObjectPortal mithraObjectPortal = address.zGetPortal();
        final MithraDatabaseObject databaseObject = mithraObjectPortal.getDatabaseObject();

        String schema = ((MithraCodeGeneratedDatabaseObject) databaseObject).getSchemaGenericSource(null);

        CHECKER.checkAndWaitForSyslogSynchronized(null, schema, databaseObject);
    }

    public void testStringSourceAttribute()
    {
        SpecialAccount specialAccount = SpecialAccountFinder.findByPrimaryKey(1, 10, "A");
        CHECKER.checkAndWaitForSyslog(specialAccount);

        MithraObjectPortal mithraObjectPortal = specialAccount.zGetPortal();
        MithraDatabaseObject databaseObject = mithraObjectPortal.getDatabaseObject();

        Attribute sourceAttribute = mithraObjectPortal.getFinder().getSourceAttribute();
        Object sourceAttributeValue = sourceAttribute.valueOf(specialAccount);
        String schema = ((MithraCodeGeneratedDatabaseObject) databaseObject).getSchemaGenericSource(sourceAttributeValue);
        CHECKER.checkAndWaitForSyslogSynchronized(sourceAttributeValue, schema, databaseObject);
    }

    public void testIntSourceAttribute()
    {
        Employee employee = EmployeeFinder.findByPrimaryKey(1, 0);
        CHECKER.checkAndWaitForSyslog(employee);

        MithraObjectPortal mithraObjectPortal = employee.zGetPortal();
        MithraDatabaseObject databaseObject = mithraObjectPortal.getDatabaseObject();
        IntegerAttribute sourceAttribute = (IntegerAttribute) mithraObjectPortal.getFinder().getSourceAttribute();
        int sourceAttributeValue = Integer.valueOf(sourceAttribute.valueOf(employee));
        String schema = ((MithraCodeGeneratedDatabaseObject) databaseObject).getSchemaGenericSource(sourceAttributeValue);
        CHECKER.checkAndWaitForSyslogSynchronized(sourceAttributeValue, schema, databaseObject);
    }
}
