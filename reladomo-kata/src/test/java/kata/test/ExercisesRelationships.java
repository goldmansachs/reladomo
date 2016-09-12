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

import com.gs.collections.api.list.MutableList;
import com.gs.collections.api.set.primitive.IntSet;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.factory.primitive.IntSets;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.collections.impl.test.Verify;
import com.gs.collections.impl.tuple.Tuples;
import com.gs.fw.common.mithra.MithraManagerProvider;
import kata.domain.Customer;
import kata.domain.CustomerAccountFinder;
import kata.domain.CustomerAccountList;
import kata.domain.CustomerFinder;
import kata.domain.CustomerList;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExercisesRelationships
        extends AbstractMithraTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExercisesRelationships.class);


    @Override
    protected String[] getTestDataFilenames()
    {
        return new String[] {"test/data_CustomersAndAccounts.txt"}; // Look into this file to see the test data being used
    }



// Create a relationship on Customer called "accounts".
// When you regenerate, this should create a getAccounts() method on Customer, and an accounts() relationship in CustomerFinder.
// Use this to answer the following questions.
// Also make this relationship bi-directional so that it also creates a customer() relationship on CustomerAccount.
// Question:
// What is the impact on the footprint of Mithra when you add a new relationship?
// Answer: NONE! Relationships are not intrusive and don't cause any extra objects to be loaded.



//------------------------ Question 1 --------------------------------------------------------
// Get all accounts of a particular customer.

    public CustomerAccountList getAccountsForCustomer(Customer customer)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ1()
    {
        Customer customer = CustomerFinder.findOne(CustomerFinder.customerId().eq(1));
        CustomerAccountList customerAccounts = this.getAccountsForCustomer(customer);
        Verify.assertSize(3, customerAccounts);

        Assert.assertNotNull(CustomerFinder.getRelatedFinderByName("accounts"));
    }




//------------------------ Question 2 --------------------------------------------------------
// Get all customers that have accounts of a particular type.

    public CustomerList getCustomersThatHaveAccountType(String accountType)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ2()
    {
        CustomerList customers = this.getCustomersThatHaveAccountType("Savings");
        Verify.assertSetsEqual(UnifiedSet.newSetWith("Yusuke Sato", "John Smith"),
                customers.asGscList().collect(CustomerFinder.name()).toSet());

        Assert.assertNotNull(CustomerAccountFinder.getRelatedFinderByName("customer"));
    }




//--------------------------- Question 3 -------------------------------------------------------
// Create another relationship on Customer that takes one parameter "accountType"
// and returns all the Accounts of the customer that have the particular account type.
// Call this relationship "accountsOfType".

    @Test
    public void testQ3()
    {
        // Implement your solution here.

        // Notes:
        // 1) The test below will not compile until you add the new relationship.
        // 2) You need to enable the test below by uncommenting the commented line
        //    once you have created the new relationship.
        // 3) Don't forget to re-run your build to see the new relationship
        //    get generated.

        Customer john = CustomerFinder.findOne(CustomerFinder.customerId().eq(1));
        CustomerAccountList savingAccounts = null;
        Assert.fail("Uncomment the following line to enable this test");
//        savingAccounts = john.getAccountsOfType("Savings");
        Verify.assertSize(2, savingAccounts);
    }




//--------------------------- Question 4 -------------------------------------------------------
// Add new customer with a list of accounts given as parameter.
// You can assume that the customer does not exist.
// Remember you will need to get Mithra to generate the new accountIds for you.

    public void addCustomerAccounts(String name, String location, MutableList<Pair<String, String>> accountDescriptionAndTypePairs)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ4()
    {
        CustomerAccountList accountsBefore = CustomerAccountFinder.findMany(CustomerAccountFinder.all());
        CustomerList customersBefore = CustomerFinder.findMany(CustomerFinder.all());
        accountsBefore.forceResolve(); //to get this list resolved before we add the new customer.
        customersBefore.forceResolve();

        MutableList<Pair<String, String>> accountDescriptionAndTypePairs = FastList.newListWith(
                Tuples.pair("Tom's saving Account", "Savings"),
                Tuples.pair("Tom's running Account", "Running")
        );

        this.addCustomerAccounts("Tom Jones", "UK", accountDescriptionAndTypePairs);


        CustomerAccountList accountsAfter = CustomerAccountFinder.findMany(CustomerAccountFinder.all());
        CustomerList customersAfter = CustomerFinder.findMany(CustomerFinder.all());

        Assert.assertEquals(1, customersAfter.size() - customersBefore.size());
        Assert.assertEquals(2, accountsAfter.size() - accountsBefore.size());

        Customer tom = CustomerFinder.findOne(CustomerFinder.name().eq("Tom Jones"));
        CustomerAccountList tomsAccounts = new CustomerAccountList(CustomerAccountFinder.customerId().eq(tom.getCustomerId()));
        Verify.assertSize(2, tomsAccounts);
    }




//------------------------- Question 5 -----------------------------------------------------------------------
// Given a list of customerIds, get a list of customers, but also deep fetch all related CustomerAccounts
// so that Mithra will not do single selects on CustomerAccount whenever we try to access an account.
// ie. if we call customers.get(0).getAccounts(); we don't want to go to the database again.
// Quiz1: How can you only deep fetch only Running accounts?
// Quiz2: Can you deep fetch without having the "accounts" relationship defined?
//
// Note: There is no good way to test whether deep fetching has happened as Mithra will perform
// single selects behind the scenes.  Best you can do is turn on sql logging and count the number
// of DB hits.

    public CustomerList getCustomers(IntSet customerIds)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ5()
    {
        IntSet accountIds = IntSets.immutable.of(1, 2, 999);
        CustomerList customers = getCustomers(accountIds);
        Verify.assertSize( 2, customers);

        int dbHitsBefore = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        // Try un-commenting the following lines, running the test, and then
        // look at the debug messages output.
//        System.out.println("No more SQL SELECT after this point ...");
//        customers.getCustomerAt(0).getAccounts().size();
//        System.out.println("----====----");
//        customers.getCustomerAt(1).getAccounts().size();
//        System.out.println("----====****====----");
        Assert.assertEquals(dbHitsBefore, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }




//------------------------- Question 6 -----------------------------------------------------------------------
// Find customers that don't have any accounts.

    public CustomerList getCustomersWithoutAccounts()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ6()
    {
        CustomerList customers = getCustomersWithoutAccounts();

        Verify.assertSetsEqual(UnifiedSet.newSetWith("Lisbeth Salander", "Mikael Blomkvist"),
                customers.asGscList().collect(CustomerFinder.name()).toSet());
    }
}
