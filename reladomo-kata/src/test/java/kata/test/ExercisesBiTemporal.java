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

import com.gs.collections.api.block.function.primitive.DoubleFunction;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.collections.impl.test.Verify;
import java.sql.Timestamp;
import kata.domain.AccountBalance;
import kata.domain.AccountBalanceList;
import kata.domain.Customer;
import kata.domain.CustomerFinder;
import kata.domain.CustomerList;
import kata.util.TimestampProvider;
import org.junit.Assert;
import org.junit.Test;

public class ExercisesBiTemporal
        extends AbstractMithraTest
{
    @Override
    protected String[] getTestDataFilenames()
    {
        return new String[] {"test/data_CustomersAccountsBalances.txt"}; // Look into this file to see the test data being used
    }



// **** NOTES ****
// (1)
// For most of the questions in this exercise you should write your own tests to verify the results
// in the style of the previous exercises.
// You should also enrich the data in test/data_Exercise3.txt so that it can cover the functionality you wish to test.
// It's _strongly_ recommended that you WRITE THE TESTS FIRST!
//
// (2)
// Different applications can have different conventions to represent the business dates and processing dates.
// In this example, we are using:
// * 18:30 as business date timestamp
// * FROM_Z<@busDate, THRU_Z> = @busDate (see toIsInclusive() in businessDate attribute
// * Infinity date = "9999-12-01 23:59:00.0", a.k.a. kata.util.TimestampProvider.getInfinityDate().
// Look at the definition of the businessDate and processingDate attribute in AccountBalance.xml
// to see how this is defined/used.
//
// (3)
// If you need a utility method to convert a date to 18:30 reference date, you can use
//     kata.util.TimestampProvider.ensure1830(Date)
// Also, you can use the ParaDate utility.
//
// (4)
// For this exercise, feel free to define your own relationships that you think can make your code easiest to implement.
//
// (5)
// Are you completely unfamiliar with chaining?
// See the Reladomo documentation



//------------------------ Question 1 --------------------------------------------------------
// Get the balance of a particular account on a particular business date.

    public double getBalanceFor(int accountId, Timestamp businessDate)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return 0.0;
    }


    @Test
    public void testQ1()
    {
        Timestamp businessDate = Timestamp.valueOf("2009-03-15 00:00:00.0");
        double balance = getBalanceFor(100, businessDate);
        Assert.assertEquals(1000.0, balance, 0.0);
    }




//------------------------ Question 2 --------------------------------------------------------
// Get the balance of a particular account on a particular business date,
// as viewed from a particular point in history (i.e. processing time).

    public double getBalanceFor(int accountId, Timestamp businessDate, Timestamp processingDate)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return 0.0;
    }


    @Test
    public void testQ2()
    {
        Timestamp businessDate = Timestamp.valueOf("2009-03-15 00:00:00.0");
        Timestamp processingDate = Timestamp.valueOf("2009-03-12 12:55:12.0");

        double balance = getBalanceFor(100, businessDate, processingDate);
        Assert.assertEquals(1060.0, balance, 0.0);
    }




//------------------------ Question 3 --------------------------------------------------------
// Get the total balance for all accounts that belong to a particular customer
// at a particular business date.
// Bonus: Can you do this using aggregate queries? (Have a look at AggregateList if you are interested)

    public double getBalanceForCustomer(int customerId, Timestamp businessDate)
    {
        Assert.fail("Implement this functionality to make the test pass");
        AccountBalanceList balances = null;
        // Using some GS Collections:
        return balances.asGscList().sumOfDouble(new DoubleFunction<AccountBalance>()
        {
            @Override
            public double doubleValueOf(AccountBalance accountBalance)
            {
                return accountBalance.getBalance();
            }
        });
    }


    @Test
    public void testQ3a()
    {
        Timestamp businessDate = Timestamp.valueOf("2009-03-15 00:00:00.0");

        double balance = getBalanceForCustomer(1, businessDate);
        Assert.assertEquals(500, balance, 0.0);
    }



    public double getBalanceForCustomerOptimizedUsingAggregate(int customerId, Timestamp businessDate)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return 0.0;
    }

    @Test
    public void testQ3b()
    {
        Timestamp businessDate = Timestamp.valueOf("2009-03-15 00:00:00.0");

        double balance = getBalanceForCustomerOptimizedUsingAggregate(1, businessDate);
        Assert.assertEquals(500, balance, 0.0);
    }




//------------------------ Question 4 --------------------------------------------------------
// Get all the AccountBalances of a particular account across all business dates.
// i.e. the history of a particular account.
// Order by businessDateFrom.

    public AccountBalanceList getAccountBalanceBusinessDateHistory(int accountNumber)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ4()
    {
        AccountBalanceList list = getAccountBalanceBusinessDateHistory(100);
        Verify.assertSize(2, list);

        AccountBalance balance1 = list.get(0);
        Assert.assertEquals(1000.0, balance1.getBalance(), 0.0);
        Assert.assertEquals(Timestamp.valueOf("2009-03-12 00:00:00.0"), balance1.getBusinessDateFrom());
        Assert.assertEquals(Timestamp.valueOf("2009-03-19 00:00:00.0"), balance1.getBusinessDateTo());

        AccountBalance balance2 = list.get(1);
        Assert.assertEquals(2000.0, balance2.getBalance(), 0.0);
        Assert.assertEquals(Timestamp.valueOf("2009-03-19 00:00:00.0"), balance2.getBusinessDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance2.getBusinessDateTo());
    }




//------------------------ Question 5 --------------------------------------------------------
// Get all the AccountBalances of a particular account on a particular business date across processing dates.
// In other words the processing date history of an account for a particular business date.
// Order by processingDateFrom.

    public AccountBalanceList getAccountBalanceProcessingDateHistory(int accountNumber, Timestamp businessDate)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ5()
    {
        Timestamp businessDate = Timestamp.valueOf("2009-03-15 00:00:00.0");

        AccountBalanceList balances = this.getAccountBalanceProcessingDateHistory(100, businessDate);

        Verify.assertSize(2, balances);

        AccountBalance balance1 = balances.get(0);
        Assert.assertEquals(1060.0d, balance1.getBalance(), 0.0);
        Assert.assertEquals(businessDate, balance1.getBusinessDate());
        Assert.assertEquals(Timestamp.valueOf("2009-03-12 12:45:12.0"), balance1.getProcessingDate());

        AccountBalance balance2 = balances.get(1);
        Assert.assertEquals(1000.0d, balance2.getBalance(), 0.0);
        Assert.assertEquals(businessDate, balance2.getBusinessDate());
        Assert.assertEquals(Timestamp.valueOf("2009-03-13 11:30:22.0"), balance2.getProcessingDate());
    }




//------------------------ Question 6 --------------------------------------------------------
// Update the balance of an account for a particular business date.
// The update should take effect from that business date onward. (i.e. THRU_Z=INFINITY).
// The method getAccountBalanceBusinessDateHistory() implemented previously, is used
// as part of the test for this question, so you must have the correct solution for that first.

    public void updateBalance(final Timestamp businessDate, final int accountId, final double newValue)
    {
        Assert.fail("Implement this method to get the test to pass");
    }


    @Test
    public void testQ6()
    {
        Timestamp startTime = new Timestamp(System.currentTimeMillis());

        Timestamp businessDate = Timestamp.valueOf("2009-03-22 00:00:00.0");

        this.updateBalance(businessDate, 100, 2040);

        Timestamp endTime = new Timestamp(System.currentTimeMillis());

        AccountBalanceList balances = this.getAccountBalanceBusinessDateHistory(100);

        Verify.assertSize(3, balances);

        AccountBalance balance1 = balances.get(0);
        Assert.assertEquals(1000.0d, balance1.getBalance(), 0.0);
        Assert.assertEquals(Timestamp.valueOf("2009-03-12 00:00:00.0"), balance1.getBusinessDateFrom());
        Assert.assertEquals(Timestamp.valueOf("2009-03-19 00:00:00.0"), balance1.getBusinessDateTo());
        Assert.assertEquals(Timestamp.valueOf("2009-03-13 11:30:22.0"), balance1.getProcessingDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance1.getProcessingDateTo());

        AccountBalance balance2 = balances.get(1);
        Assert.assertEquals(2000.0d, balance2.getBalance(), 0.0);
        Assert.assertEquals(Timestamp.valueOf("2009-03-19 00:00:00.0"), balance2.getBusinessDateFrom());
        Assert.assertEquals(businessDate, balance2.getBusinessDateTo());
        this.assertBetween(startTime, endTime, balance2.getProcessingDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance2.getProcessingDateTo());

        AccountBalance balance3 = balances.get(2);
        Assert.assertEquals(2040.0d, balance3.getBalance(), 0.0);
        Assert.assertEquals(businessDate, balance3.getBusinessDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance3.getBusinessDateTo());
        this.assertBetween(startTime, endTime, balance3.getProcessingDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance3.getProcessingDateTo());
    }




//------------------------ Question 7 --------------------------------------------------------
// Update the balance of an account for a particular business date ONLY!!
// Any subsequent business dates should stay the same as before the update
// (even if the row you are updating has businessDateTo=INFINITY).
// You can use kata.util.TimestampProvider.getNextDay to get the next day.

    public void updateBalanceWithDailyChaining(final Timestamp businessDate, final int accountId, final double newValue)
    {
        Assert.fail("Implement this method to get the test to pass");
    }


    @Test
    public void testQ7()
    {
        Timestamp startTime = new Timestamp(System.currentTimeMillis());

        Timestamp businessDate = Timestamp.valueOf("2009-03-15 00:00:00.0");

        this.updateBalanceWithDailyChaining(businessDate, 100, 1099);

        Timestamp endTime = new Timestamp(System.currentTimeMillis());

        AccountBalanceList balances = this.getAccountBalanceBusinessDateHistory(100);

        Verify.assertSize(4, balances);

        AccountBalance balance1 = balances.get(0);
        Assert.assertEquals(1000.0d, balance1.getBalance(), 0.0);
        Assert.assertEquals(Timestamp.valueOf("2009-03-12 00:00:00.0"), balance1.getBusinessDateFrom());
        Assert.assertEquals(businessDate, balance1.getBusinessDateTo());
        this.assertBetween(startTime, endTime, balance1.getProcessingDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance1.getProcessingDateTo());

        Timestamp nextDay = TimestampProvider.getNextDay(businessDate);

        AccountBalance balance2 = balances.get(1);
        Assert.assertEquals(1099.0d, balance2.getBalance(), 0.0);
        Assert.assertEquals(businessDate, balance2.getBusinessDateFrom());
        Assert.assertEquals(nextDay, balance2.getBusinessDateTo());
        this.assertBetween(startTime, endTime, balance2.getProcessingDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance2.getProcessingDateTo());

        AccountBalance balance3 = balances.get(2);
        Assert.assertEquals(1000.0d, balance3.getBalance(), 0.0);
        Assert.assertEquals(nextDay, balance3.getBusinessDateFrom());
        Assert.assertEquals(Timestamp.valueOf("2009-03-19 00:00:00.0"), balance3.getBusinessDateTo());
        this.assertBetween(startTime, endTime, balance3.getProcessingDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance3.getProcessingDateTo());

        AccountBalance balance4 = balances.get(3);
        Assert.assertEquals(2000.0d, balance4.getBalance(), 0.0);
        Assert.assertEquals(Timestamp.valueOf("2009-03-19 00:00:00.0"), balance4.getBusinessDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance4.getBusinessDateTo());
        Assert.assertEquals(Timestamp.valueOf("2009-03-13 19:35:12.0"), balance4.getProcessingDateFrom());
        Assert.assertEquals(TimestampProvider.getInfinityDate(), balance4.getProcessingDateTo());
    }




//------------------------ Question 8 --------------------------------------------------------
// Get all customers who have a net negative balance for a particular business date.
// (in other words, whose sum of balances for all their account is less than zero)

// First, attempt this by fetching balances into memory, and then figuring out the totals.

    public MutableList<Customer> getCustomersWithNegativeNetBalanceInMemory(Timestamp businessDate)
    {
        Assert.fail("Implement this method to get the test to pass");
        return null;
    }


    @Test
    public void testQ8a()
    {
        Timestamp mid2008 = Timestamp.valueOf("2008-06-01 00:00:00.0");
        MutableList<Customer> inDebt2008 = getCustomersWithNegativeNetBalanceInMemory(mid2008);
        Verify.assertListsEqual(FastList.newListWith("Bernie Madoff"),
                inDebt2008.collect(CustomerFinder.name()));

        Timestamp now = new Timestamp(System.currentTimeMillis());
        MutableList<Customer> inDebtNow = getCustomersWithNegativeNetBalanceInMemory(now);
        Verify.assertSetsEqual(UnifiedSet.newSetWith("Bernie Madoff", "Ken Lay", "Yusuke Sato"),
                inDebtNow.collect(CustomerFinder.name()).toSet());
    }



// Second, attempt the same thing, but this time use AggregateList to calculate the balance totals
// and only return customers with negative total balances.

    public CustomerList getCustomersWithNegativeNetBalanceUsingAggregate(Timestamp businessDate)
    {
        Assert.fail("Implement this method to get the test to pass");
        return null;
    }


    @Test
    public void testQ8b()
    {
        Timestamp mid2008 = Timestamp.valueOf("2008-06-01 00:00:00.0");
        final CustomerList inDebt2008 = getCustomersWithNegativeNetBalanceUsingAggregate(mid2008);
        Verify.assertListsEqual(FastList.newListWith("Bernie Madoff"),
                inDebt2008.asGscList().collect(CustomerFinder.name()));

        Timestamp now = new Timestamp(System.currentTimeMillis());
        final CustomerList inDebtNow = getCustomersWithNegativeNetBalanceUsingAggregate(now);
        Verify.assertSetsEqual(UnifiedSet.newSetWith("Bernie Madoff", "Ken Lay", "Yusuke Sato"),
                inDebtNow.asGscList().collect(CustomerFinder.name()).toSet());
    }




//---------------------------------------------------------------------------------
// Convenience methods and selectors to help make testing easier!

    /**
     * A little helper method to assert that a give Timestamp is between two other timestamps.
     */
    private void assertBetween(Timestamp expectedLowerBound, Timestamp expectedUpperBound, Timestamp actualTimestamp)
    {
        long actualTime = actualTimestamp.getTime() / 10; // drop a tiny amount of time that gets rounded down by the DB
        long expectedLowerBoundTime = expectedLowerBound.getTime() / 10;
        if (actualTime < expectedLowerBoundTime || actualTimestamp.after(expectedUpperBound))
        {
            Assert.fail("Timestamp <" + actualTimestamp + "> not within "
                    + "expected range <" + expectedLowerBound + ">-<" + expectedUpperBound + '>');
        }
        Assert.assertTrue(actualTimestamp.before(expectedUpperBound) || actualTimestamp.equals(expectedUpperBound));
    }
}
