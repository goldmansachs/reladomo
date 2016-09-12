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

package kata.test;

import java.sql.Timestamp;
import kata.domain.Customer;
import kata.domain.CustomerAccountList;
import org.junit.Assert;
import org.junit.Test;

public class ExercisesObjectFromScratch
        extends AbstractMithraTest
{
    @Override
    protected String[] getTestDataFilenames()
    {
        return new String[]{"test/data_ObjectFromScratch.txt"}; // Look here to see the data for this test
    }


    /*
     * For this exercise you need to introduce a new mithra Class.
     * This will represent the AccountLimit.
     * AccountLimit will represent the limit of an account for a particular business date.
     * This should be a DATED object (FROM_Z, THRU_Z, IN_Z, OUT_Z),
     * whose key will be the accountId, and have a double "limit" attribute.
     * A customer can talk to his Bank representative to change his/her account limit,
     * but the change will only take effect from the date of the change onwards,
     * such that any charges for going over the limit that happened in the past
     * can still get calculated after the task.
     *
     *
     * To add the object successfully make sure that:
     *  - You create the xml correctly.
     *  - You get the Mithra Code generator to pick up your xml.
     *  - You register the new object with the mithra runtime config XML
     *    and set the appropriate caching policy for the object.
     *
     *
     * You will need to add test data that will make sense for your tests in:
     *    test/data_ObjectFromScratch.txt
     *
     *
     * Add any relationships that make sense.
     * You are encouraged to add the following:
     *  - AccountLimit - CustomerAccount (relationship between dated and non-dated objects)
     *  - AccountLimit - AccountBalance (relationship between two dated object)
     */


//---------------------- Question 1 ---------------------------------------------
// Get all accounts who have exceeded their balance for a particular day.

    public CustomerAccountList getAccountsOverTheLimit(Timestamp businessDate)
    {
        Assert.fail("Implement this method to get the test to pass");
        return null;
    }

    @Test
    public void testQ1()
    {
        Assert.fail("Implement to test the behaviour in this question");
    }

// --------------------- Question 2 ----------------------------------------------
// For a particular customer, get the net balance over the limit
// for a particular business date.
// If the customer has two accounts, one which is $5 over the limit
// and the other one is $6 under the limit the result should be 5 - 6 = -1.

    public double getNetBalanceOverLimit(Customer customer, Timestamp businessDate)
    {
        Assert.fail("Implement this method to get the test to pass");
        return 0;
    }

    @Test
    public void testQ2()
    {
        Assert.fail("Implement to test the behaviour in this question");
    }

// ---------------------- Question 3+ ------------------------------------------
// Feel free to write more tests that will help you experiment with the
// different Mithra features.


}