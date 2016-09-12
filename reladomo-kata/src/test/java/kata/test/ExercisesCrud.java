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
import com.gs.collections.impl.test.Verify;
import com.gs.collections.impl.tuple.Tuples;
import com.gs.fw.common.mithra.MithraManager;
import kata.domain.Customer;
import kata.domain.CustomerFinder;
import kata.domain.CustomerList;
import org.junit.Assert;
import org.junit.Test;

public class ExercisesCrud
        extends AbstractMithraTest
{
    @Override
    protected String[] getTestDataFilenames()
    {
        return new String[] {"test/data_Customer.txt"}; // Look into this file to see the test data being used
    }



//-------------- Question 1 ------------------------------------------------------
// Get customer with a particular ID.


    public Customer getCustomer(int customerId)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ1()
    {
        Customer customer = this.getCustomer(0);
        Assert.assertEquals("Hiro Tanaka", customer.getName());
        Assert.assertEquals("JPN", customer.getCountry());
    }




//----------  Question 2  -----------------------------------------------------
// Get all customers from a particular country
//
// Quiz: At which point does mithra actually evaluate the list by going to the database?
//       Is it when you create the list or when you use it?
// Answer: Only when you use it.  This typically means when you call .get(), .iterator(), .size(),
//       but there are a couple other methods that will also cause the fetch query to be executed.
//       Feel free to experiment to see how this works

    public CustomerList getCustomersFrom(String country)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ2()
    {
        CustomerList customer = getCustomersFrom("JPN");
        Verify.assertSize(4, customer);
    }




//------------ Question 3 ------------------------------------------------------
// Get all customers:
//      from a particular country
// AND  whose customer names start with particular String

    public CustomerList getCustomersFromACountryWhoseNamesStartWith(String country, String startWith)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ3()
    {
        CustomerList list = getCustomersFromACountryWhoseNamesStartWith("JPN", "Yu");
        list.setOrderBy(CustomerFinder.name().ascendingOrderBy().and(CustomerFinder.customerId().descendingOrderBy()));
        Verify.assertSize(3, list);
        Assert.assertEquals("Yuki Suzuki", list.get(0).getName());
        Assert.assertEquals("Yuri Clark", list.get(1).getName());
        Assert.assertEquals("Yusuke Sato", list.get(2).getName());
    }




//----------------- Question 4 ----------------------------------------------
// Add new customer and return the customer ID.
// Hint! How are you going to create a new  customerID? Can Mithra create it for you?
// You might need to change something in the Customer.xml file.

    public int addNewCustomer(String name, String country)
    {
        Verify.fail("Implement this functionality to make the test pass");
        return 0;
    }


    @Test
    public void testQ4()
    {
        Customer kenny = CustomerFinder.findOne(CustomerFinder.name().eq("Kenny Rogers"));
        Assert.assertNull(kenny);

        int newId = addNewCustomer("Kenny Rogers", "USA");

        Assert.assertTrue(newId != 0);
        Customer newKenny = CustomerFinder.findOne(CustomerFinder.name().eq("Kenny Rogers"));
        Assert.assertNotNull(newKenny);
        Assert.assertEquals(newId, newKenny.getCustomerId());
        Assert.assertEquals("USA", newKenny.getCountry());
    }




//------------------- Question 5 -----------------------------------------------
// Update the country of a list of Customers.
// You can assume that the list is one that arrives in the <code>customers</code> parameter.

    public void updateCountry(CustomerList customers, String newCountry)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ5()
    {
        IntSet testIds = IntSets.immutable.of(1, 2);
        CustomerList customers = new CustomerList(CustomerFinder.customerId().in(testIds));
        this.updateCountry(customers, "UK");

        MithraManager.getInstance().clearAllQueryCaches();
        Customer c1  = CustomerFinder.findOne(CustomerFinder.customerId().eq(1));
        Customer c2  = CustomerFinder.findOne(CustomerFinder.customerId().eq(2));

        //check that the two got updated
        Assert.assertEquals("UK", c1.getCountry());
        Assert.assertEquals("UK", c2.getCountry());

        //check that nobody else got updated.
        Verify.assertSize(2, new CustomerList(CustomerFinder.country().eq("UK")));
        Verify.assertSize(3, new CustomerList(CustomerFinder.country().notEq("UK")));
    }




//------------------- Question 6 -----------------------------------------------
// Change the country of all customers from one value to another.

    public void updateCountryFromTo(String from, String to)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ6()
    {
        updateCountryFromTo("JPN", "JP");
        CustomerList jpn = new CustomerList(CustomerFinder.country().eq("JPN"));
        Verify.assertSize( 0, jpn);

        CustomerList jp  = new CustomerList(CustomerFinder.country().eq("JP"));
        Verify.assertSize( 4, jp);
    }




//------------------- Question 7 -----------------------------------------------
// Add two customers in one go.  Both will have the same country.
// Because of legal requirements, either both must be added, or if there is a problem,
// then neither get added.
// You'll need to use a "transaction" to achieve this functionality.
// Use the supplied method <code>possiblySimulateProblem(String country)</code>
// as a way of sometimes simulating a "problem" occurring, by putting it
// between the first and second inserts.

    public void addTwoCustomers(final String name1, final String name2, final String country)
    {
        Assert.fail("Implement this functionality to make the test pass");

        // your code here to insert first customer

        possiblySimulateProblem(country);

        // your code here to insert second customer
    }

    private void possiblySimulateProblem(String country)
    {
        if (country.equals("XX"))
        {
            throw new RuntimeException("Simulated problem");
        }
    }


    @Test
    public void testQ7()
    {
        int countBefore = CustomerFinder.findMany(CustomerFinder.all()).count();

        Verify.assertThrows(RuntimeException.class, new Runnable()
        {
            public void run()
            {
                addTwoCustomers("Romulus", "Remus", "XX");
            }
        });

        Assert.assertEquals(countBefore, CustomerFinder.findMany(CustomerFinder.all()).count());

        this.addTwoCustomers("Tweedle-Dee", "Tweedle-Dum", "DE");

        Assert.assertEquals(countBefore + 2, CustomerFinder.findMany(CustomerFinder.all()).count());
    }




//------------------- Question 8 -----------------------------------------------
// Add  new customers in a batch given {"name", "country"} pairs.
// You can assume that none of the new customers exist.

    public void addNewCustomers(MutableList<Pair<String, String>> newCustomerInfo)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ8()
    {
        int sizeBefore = CustomerFinder.findMany(CustomerFinder.all()).size();
        Pair<String,String> customer1 = Tuples.pair("John Courage", "UK");
        Pair<String,String> customer2 = Tuples.pair("Tony Jackson", "USA");
        this.addNewCustomers(FastList.newListWith(customer1, customer2));
        int sizeAfter = CustomerFinder.findMany(CustomerFinder.all()).size();
        Assert.assertEquals(2, sizeAfter - sizeBefore);

        Assert.assertNotNull(CustomerFinder.findOne(CustomerFinder.name().eq("John Courage")));
        Assert.assertNotNull(CustomerFinder.findMany(CustomerFinder.name().eq("Tony Jackson")));
    }




//------------------- Question 9 -----------------------------------------------
// Delete a Customer given the customerId.

    public void deleteCustomer(int customerId)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ10()
    {
        int sizeBefore = CustomerFinder.findMany(CustomerFinder.all()).size();

        deleteCustomer(1);

        int sizeAfter = CustomerFinder.findMany(CustomerFinder.all()).size();
        Assert.assertEquals(-1, sizeAfter - sizeBefore);
    }




//------------------- Question 11 -----------------------------------------------
// Delete a list of customers.
// Try to do it in a batch, not one by one.

    public void deleteCustomers(IntSet customers)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ11()
    {
        int sizeBefore = CustomerFinder.findMany(CustomerFinder.all()).size();
        IntSet customerIds = IntSets.immutable.of(1, 2);
        deleteCustomers(customerIds);

        int sizeAfter = CustomerFinder.findMany(CustomerFinder.all()).size();
        Assert.assertEquals(-2, sizeAfter - sizeBefore);
    }




//------------------- Question 12 -----------------------------------------------
// Add a unique index on Customer name, using Mithra.  Call it "nameIndex".
// Hint: This can be done in the Mithra object XML...

    @Test
    public void testQ12()
    {
        // Hard to test, since the MithraTestResource doesn't apply a "unique index" to the H2 DB for us ...
        try
        {
            Assert.assertNotNull(CustomerFinder.class.getDeclaredMethod("findByNameIndex", String.class));
        }
        catch (NoSuchMethodException e)
        {
            Assert.fail("Implement a unique index on the Customer object to make this test pass");
        }
    }




//------------------- Question 13 -----------------------------------------------
// Use your new index to easily look up information given a Customers unique name.

    public Customer getByCustomerName(String customerName)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ13()
    {
        Assert.assertEquals(2, this.getByCustomerName("Yusuke Sato").getCustomerId());
        Assert.assertEquals(1, this.getByCustomerName("John Smith").getCustomerId());
    }
}
