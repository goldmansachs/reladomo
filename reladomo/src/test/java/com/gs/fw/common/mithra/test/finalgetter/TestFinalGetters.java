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

package com.gs.fw.common.mithra.test.finalgetter;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import junit.framework.TestCase;

import com.gs.fw.common.mithra.test.domain.AccountAbstract;
import com.gs.fw.common.mithra.test.domain.AccountTransactionAbstract;
import com.gs.fw.common.mithra.test.domain.ParaBalanceAbstract;
import com.gs.fw.common.mithra.test.domain.ParaPositionAbstract;
import com.gs.fw.common.mithra.test.domain.PersonAbstract;
import com.gs.fw.common.mithra.test.domain.evo.EvoType1DatedReadOnlyTypesAbstract;
import com.gs.fw.common.mithra.test.domain.evo.EvoType1DatedTxnTypesAbstract;
import com.gs.fw.common.mithra.test.domain.evo.EvoType1ReadOnlyTypesAbstract;
import com.gs.fw.common.mithra.test.domain.evo.EvoType1TxnTypesAbstract;
import com.gs.fw.common.mithra.test.glew.LewProductAbstract;


public class TestFinalGetters extends TestCase
{
    public void testGeneratorSwitchOn() throws NoSuchMethodException
    {
        for (Class parentClass : Arrays.asList(FinalParentAbstract.class, TxFinalParentAbstract.class))
        {
            assertNotFinal(parentClass, "Id", true);
            assertFinal(parentClass, "ChildId", true); 
            assertFinal(parentClass, "Child", false); 
            assertNotFinal(parentClass, "Embedded", false); 
            assertFinal(parentClass, "EmbeddedValue", true); 
            assertFinal(parentClass, "EmbeddedNested", false); 
            assertNotFinal(parentClass, "EmbeddedNestedValue", true); 
            assertFinal(parentClass, "Region", true); 
            assertNotFinal(parentClass, "BusinessDate", false); 
            assertNotFinal(parentClass, "BusinessDateTo", true); 
            assertNotFinal(parentClass, "BusinessDateFrom", true); 
            assertFinal(parentClass, "ProcessingDate", false); 
            assertFinal(parentClass, "ProcessingDateTo", true); 
            assertFinal(parentClass, "ProcessingDateFrom", true);
        }

        for (Class childClass : Arrays.asList(FinalChildAbstract.class, TxFinalChildAbstract.class))
        {
            assertNotFinal(childClass, "Region", true); 
            assertFinal(childClass, "Id", true); 
            assertNotFinal(childClass, "Parent", false); 
            assertFinal(childClass, "Embedded", false); 
            assertNotFinal(childClass, "EmbeddedValue", true); 
            assertNotFinal(childClass, "EmbeddedNested", false); 
            assertFinal(childClass, "EmbeddedNestedValue", true); 
        }
    }
    
    private static void assertFinal(Class aClass, String method, boolean nullable) throws NoSuchMethodException
    {
        assertTrue(aClass.getSimpleName() + "." + "get" + method, isFinal(aClass, "get" + method));
        if (nullable)
        {
            assertTrue(aClass.getSimpleName() + "." + "is" + method + "Null", isFinal(aClass, "is" + method + "Null"));
        }
    }

    private static void assertNotFinal(Class aClass, String method, boolean nullable) throws NoSuchMethodException
    {
        assertFalse(aClass.getSimpleName() + "." + "get" + method, isFinal(aClass, "get" + method));
        if (nullable)
        {
            assertFalse(aClass.getSimpleName() + "." + "is" + method + "Null", isFinal(aClass, "is" + method + "Null"));
        }
    }

    private static boolean isFinal(Class aClass, String method) throws NoSuchMethodException
    {
        return Modifier.isFinal(aClass.getDeclaredMethod(method).getModifiers());
    }

    public void testGeneratorSwitchOff() throws NoSuchMethodException
    {
        // dated read-only
        assertFinal(LewProductAbstract.class, "Region", true);
        assertNotFinal(LewProductAbstract.class, "BusinessDate", false);
        assertNotFinal(LewProductAbstract.class, "BusinessDateTo", true);
        assertNotFinal(LewProductAbstract.class, "BusinessDateFrom", true);
        assertFinal(LewProductAbstract.class, "ProcessingDate", false);
        assertFinal(LewProductAbstract.class, "ProcessingDateTo", true);
        assertFinal(LewProductAbstract.class, "ProcessingDateFrom", true);
        assertNotFinal(LewProductAbstract.class, "InstrumentId", true);
        assertFinal(LewProductAbstract.class, "Role", true);
        assertFinal(LewProductAbstract.class, "RelationshipWithLeftFitler", false);
        
        assertFinal(EvoType1DatedReadOnlyTypesAbstract.class, "Pk", false);
        assertNotFinal(EvoType1DatedReadOnlyTypesAbstract.class, "RootEvo", false);

        
        
        // simple read-only
        assertFinal(AccountAbstract.class, "DeskId", true); // source attr
        assertFinal(AccountAbstract.class, "AccountNumber", true);
        assertFinal(AccountAbstract.class, "Code", true);
        assertNotFinal(AccountAbstract.class, "TrialId", true);
        assertNotFinal(AccountAbstract.class, "PnlGroupId", true);
        assertFinal(AccountAbstract.class, "TamsAccount", false);
        assertNotFinal(AccountAbstract.class, "Trial", false);
        
        assertFinal(EvoType1ReadOnlyTypesAbstract.class, "Pk", false);
        assertNotFinal(EvoType1ReadOnlyTypesAbstract.class, "RootEvo", false);

        
        
        // dated transactional
        assertFinal(ParaBalanceAbstract.class, "AcmapCode", true); // source attr
        assertFinal(ParaBalanceAbstract.class, "BusinessDate", false);
        assertFinal(ParaBalanceAbstract.class, "BusinessDateFrom", true);
        assertFinal(ParaBalanceAbstract.class, "BusinessDateTo", true);
        assertNotFinal(ParaBalanceAbstract.class, "ProcessingDate", false);
        assertNotFinal(ParaBalanceAbstract.class, "ProcessingDateFrom", true);
        assertNotFinal(ParaBalanceAbstract.class, "ProcessingDateTo", true);
        assertFinal(ParaBalanceAbstract.class, "BalanceId", true);
        assertNotFinal(ParaBalanceAbstract.class, "Quantity", true);

        assertNotFinal(ParaPositionAbstract.class, "Product", false);
        assertFinal(ParaPositionAbstract.class, "Account", false);

        assertFinal(EvoType1DatedTxnTypesAbstract.class, "Pk", false);
        assertNotFinal(EvoType1DatedTxnTypesAbstract.class, "RootEvo", false);



        // simple transactional
        assertFinal(AccountTransactionAbstract.class, "DeskId", true); // source attr
        assertFinal(AccountTransactionAbstract.class, "TransactionId", true);
        assertFinal(AccountTransactionAbstract.class, "TransactionDescription", true);
        assertNotFinal(AccountTransactionAbstract.class, "TransactionDate", true);

        assertFinal(PersonAbstract.class, "Firm", false);
        assertFalse(Modifier.isFinal(PersonAbstract.class.getDeclaredMethod("getAddress", String.class).getModifiers()));

        assertFinal(EvoType1TxnTypesAbstract.class, "Pk", false);
        assertNotFinal(EvoType1TxnTypesAbstract.class, "RootEvo", false);
    }
}
