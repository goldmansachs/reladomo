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
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class TestComplexKeyRelationship
extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Underlier.class,
            Contract.class
        };
    }

    public void testMultiInOperation()
    throws Exception
    {
        String sql;
        List underliers;

        sql = "select u.* from UNDERLIER u, CONTRACT c where u.CONTRACT_TYPE = c.CONTRACT_TYPE and u.CONTRACT_ID = c.CONTRACT_ID and u.PRODUCT_ID = c.PRODUCT_ID and u.CURRENCY = 'GBP' and c.CONTRACT_TYPE = 'Swap'";
        underliers = new UnderlierList(UnderlierFinder.currency().eq("GBP").and(UnderlierFinder.contract().contractType().eq("Swap")));
        this.genericRetrievalTest(sql, underliers);
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new UnderlierResultSetComparator());
    }

    public void testRelationshipWithMappedOperation() throws SQLException
    {
        ContractList cl = new ContractList(ContractFinder.underliers().currency().eq("USD"));
        cl.deepFetch(ContractFinder.underliers());
        cl.forceResolve();
        int oldCount = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        for(Iterator it = cl.iterator(); it.hasNext(); )
        {
            Contract contract = (Contract) it.next();
            String sql = "select u.* from UNDERLIER u, CONTRACT c where u.CONTRACT_TYPE = c.CONTRACT_TYPE and " +
                    "u.CONTRACT_ID = c.CONTRACT_ID and u.PRODUCT_ID = c.PRODUCT_ID " +
                    " and c.CONTRACT_TYPE = '"+contract.getContractType()+"' "+
                    " and c.CONTRACT_ID = "+contract.getContractId()+
                    " and c.ACCOUNT_ID = '"+contract.getAccountId()+"'"+
                    " and c.PRODUCT_ID = "+contract.getProductId();
            this.genericRetrievalTest(sql, contract.getUnderliers());
        }
        assertEquals(oldCount, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }


}
