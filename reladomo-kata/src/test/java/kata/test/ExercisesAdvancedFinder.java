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

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.api.set.MutableSet;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.api.tuple.Twin;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.collections.impl.test.Verify;
import com.gs.collections.impl.tuple.Tuples;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import kata.domain.AccountBalanceFinder;
import kata.domain.AllTypes;
import kata.domain.AllTypesFinder;
import kata.domain.Customer;
import kata.domain.CustomerAccount;
import kata.domain.CustomerAccountFinder;
import kata.domain.CustomerAccountList;
import kata.domain.CustomerFinder;
import kata.domain.Person;
import kata.domain.PersonFinder;
import kata.domain.PersonList;
import org.junit.Assert;
import org.junit.Test;

public class ExercisesAdvancedFinder
        extends AbstractMithraTest
{
    @Override
    protected String[] getTestDataFilenames()
    {
        // Look into these files to see the test data being used
        return new String[]{"test/data_Person.txt", "test/data_CustomersAndAccounts.txt"};
    }




//-------------- Question 1 ------------------------------------------------------
// Given a Customer, find their list of accounts
// *without* using customer.getAccounts().

    public CustomerAccountList getAccountsByCustomer(Customer customer)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ1()
    {
        Customer suzukiSan = CustomerFinder.findOne(CustomerFinder.name().eq("Yuki Suzuki"));

        CustomerAccountList accounts = this.getAccountsByCustomer(suzukiSan);

        Verify.assertSize(1, accounts);
        Assert.assertEquals("My Account", accounts.get(0).getAccountName());
        Assert.assertEquals("Running", accounts.get(0).getAccountType());
    }




//-------------- Question 2 ------------------------------------------------------
// Find only certain CustomerAccounts based on the pairs of account names and account types.

    public CustomerAccountList getAccountsBasedOnTuples(MutableList<Twin<String>> accountNameAndTypes)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ2()
    {
        MutableList accountNameAndTypes = FastList.newListWith(
                Tuples.twin("John's Saving Account 2", "Savings"),
                Tuples.twin("My Account", "Running"),
                Tuples.twin("No customer Account", "Virtual")
        );

        CustomerAccountList accounts = this.getAccountsBasedOnTuples(accountNameAndTypes);

        Verify.assertListsEqual(FastList.newListWith(300, 500, 600),
                accounts.asGscList().collect(CustomerAccountFinder.accountId()));
    }
    



//-------------- Question 3 ------------------------------------------------------
// Count how many person objects there are.
// Do this without fetching every row from the DB.
// Note:  You can check how many rows you are fetching
//        by looking at the SQL debug console output.

    public int countPeople()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return 0;
    }


    @Test
    public void testQ3()
    {
        Assert.assertEquals(16, this.countPeople());
    }




//-------------- Question 4 ------------------------------------------------------
// Find the sum of ages for all Person objects.  This is a two stage question:
// 1) Find the sum without using any special Mithra features.
// 2) Find the sum using Mithra's AggregateList feature.
// Note: When done with both questions, look at the SQL debug console output,
//       and note the difference in how much data Mithra had to move from the
//       the DB to your application to get the answer!

    public int getSumAgeNormalMithra()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return 0;
    }

    @Test
    public void testQ4a()
    {
        Assert.assertEquals(556, this.getSumAgeNormalMithra());
    }



    public double getSumAgeUsingAggregateList()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return 0;
    }

    @Test
    public void testQ4b()
    {
        Assert.assertEquals(556, this.getSumAgeUsingAggregateList(), 0.0);
        final Person person = new Person();
        person.setName("Michael Caine");
        person.setCountry("USA");
        person.setAge(65);
        person.insert();
        Assert.assertEquals(this.getSumAgeNormalMithra(), this.getSumAgeUsingAggregateList(), 0.0);
    }




//-------------- Question 5 ------------------------------------------------------
// Find the average age of all Person objects.  This is a two stage question:
// 1) Find the average without using any special Mithra features.
// 2) Find the average using Mithra's AggregateList feature.

    public double getAverageAgeNormalMithra()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return 0;
    }

    @Test
    public void testQ5a()
    {
        Assert.assertEquals(34.75, this.getAverageAgeNormalMithra(), 0.0);
    }



    public double getAverageAgeUsingAggregateList()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return 0;
    }

    @Test
    public void testQ5b()
    {
        Assert.assertEquals(34.75, this.getAverageAgeUsingAggregateList(), 0.0);
        final Person person = new Person();
        person.setName("Michael Caine");
        person.setCountry("USA");
        person.setAge(65);
        person.insert();
        Assert.assertEquals(this.getAverageAgeNormalMithra(), this.getAverageAgeUsingAggregateList(), 0.0);
    }




//-------------- Question 6 ------------------------------------------------------
// Find the range of ages of people.  Do this as a single hit to the DB.

    public Twin<Integer> getAgeRange()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return Tuples.twin(-1, 999);
    }


    @Test
    public void testQ6()
    {
        final Twin<Integer> ageRange = this.getAgeRange();
        Assert.assertEquals("Youngest age", 5, ageRange.getOne().intValue());
        Assert.assertEquals("Oldest age", 100, ageRange.getTwo().intValue());
    }




//-------------- Question 7 ------------------------------------------------------
// Get the count of number of people, by country, and return as a Map.

    public MutableMap<String, Integer> getCountOfPeopleByCountry()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ7()
    {
        MutableMap<String, Integer> peopleByCountry = this.getCountOfPeopleByCountry();

        MutableMap<String, Integer> expectedResult = UnifiedMap.newMapWith(
                Tuples.pair("USA", 4),
                Tuples.pair("AU", 3),
                Tuples.pair("DE", 2),
                Tuples.pair("UK", 2),
                Tuples.pair("JPN", 4),
                Tuples.pair("MA", 1)
        );
        Verify.assertMapsEqual(expectedResult, peopleByCountry);
    }




//-------------- Question 8 ------------------------------------------------------
// Fetch People, ordered by their personId.
// For each person, count the number of letters in their name.
// Stop counting when you reach stopAtPersonNamed (but include them in the count),
// and return the total.
// *Do not* fetch all rows, but *do* fetch them efficiently.

    public int countLettersOfNamesUpTo(final String stopAtPersonNamed)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return 0;
    }


    @Test
    public void testQ8()
    {
        Assert.assertEquals(63, this.countLettersOfNamesUpTo("Clark Kent"));
        Assert.assertEquals(152, this.countLettersOfNamesUpTo("Donkey Kong"));
        Assert.assertEquals(174, this.countLettersOfNamesUpTo("Does Not Exist"));
    }




//-------------- Question 9 ------------------------------------------------------
// Add a method to PersonList to increment each Person's age by 1.

    public void incrementAges(PersonList people)
    {
        Assert.fail("Uncomment the following line, then go implement the method");
//        people.incrementAges();
    }


    @Test
    public void testQ9()
    {
        this.incrementAges(new PersonList(PersonFinder.all()));
        Assert.assertEquals(53, PersonFinder.findOne(PersonFinder.name().eq("Kent Beck")).getAge());
        Assert.assertEquals(35.75, this.getAverageAgeUsingAggregateList(), 0.0);

        this.incrementAges(new PersonList(PersonFinder.name().contains("i")));
        Assert.assertEquals(53, PersonFinder.findOne(PersonFinder.name().eq("Kent Beck")).getAge());
        Assert.assertEquals(38, PersonFinder.findOne(PersonFinder.name().eq("John Smith")).getAge());
        Assert.assertEquals(7, PersonFinder.findOne(PersonFinder.name().eq("Baby Finster")).getAge());
        Assert.assertEquals(36.25, this.getAverageAgeUsingAggregateList(), 0.0);
    }




//-------------- Question 10 ------------------------------------------------------
// Return a set of all the Mithra attribute names for a given object type.

    public MutableSet<String> getAttributeNamesFor(AbstractRelatedFinder finder)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ10()
    {
        Verify.assertSetsEqual(UnifiedSet.newSetWith("personId", "name", "country", "age"),
                this.getAttributeNamesFor(PersonFinder.getFinderInstance()));

        Verify.assertSetsEqual(UnifiedSet.newSetWith("businessDateFrom", "businessDateTo", "processingDateFrom", "processingDateTo", "accountId", "balance"),
                this.getAttributeNamesFor(AccountBalanceFinder.getFinderInstance()));
    }




//-------------- Question 11 ------------------------------------------------------
// For a given object type, find all the String attributes, and return a map of
// the attribute name to the max length that the column can store.
// Note: Sometimes it is nice to be able to feed the DB max-length through to the UI,
//       so that the UI can render and/or enforce a text-box of the correct input size.

    public MutableMap<String, Integer> getStringToMaxLength(AbstractRelatedFinder finder)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ11()
    {
        Verify.assertMapsEqual(
                UnifiedMap.newMapWith(Tuples.pair("name", 64), Tuples.pair("country", 48)),
                this.getStringToMaxLength(CustomerFinder.getFinderInstance()));
    }




//-------------- Question 12 ------------------------------------------------------
// Given a set of Integers representing Person IDs, find the corresponding people.
// Avoid:
//   * looping over the IDs to fetch the people one-by-one
//   * adding the IDs individually to a new IntSet ... 

    public PersonList getPeopleIn(MutableSet<Integer> peopleIds)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ12()
    {
        final PersonList people = this.getPeopleIn(UnifiedSet.newSetWith(3, 9, 15, 99));
        Verify.assertSetsEqual(UnifiedSet.newSetWith("Yuki Suzuki", "Douglas Adams", "Jango Fett"),
                people.asGscList().collect(PersonFinder.name()).toSet());
    }




//-------------- Question 13 ------------------------------------------------------
// Given an array of Mithra attributes, and a Mithra object,
// return a list representing the values indicated by the attributes for the object.
// Do not use Reflection.

    public MutableList<Object> getAttributeValuesForObjects(
            final MithraObject mithraObject,
            Attribute... attributes)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ13()
    {
        Person ada = PersonFinder.findOne(PersonFinder.personId().eq(8));

        MutableList<Object> adaAttrs
                = this.getAttributeValuesForObjects(ada, PersonFinder.age(), PersonFinder.personId(), PersonFinder.name());

        Verify.assertListsEqual(FastList.newListWith(24, 8, "Ada Lovelace"), adaAttrs);


        Person douglas = PersonFinder.findOne(PersonFinder.personId().eq(9));

        MutableList<Object> douglasAttrs
                = this.getAttributeValuesForObjects(douglas, PersonFinder.age(), PersonFinder.name());

        Verify.assertListsEqual(FastList.newListWith(42, "Douglas Adams"), douglasAttrs);


        // Now use the exact same method for a different Mithra Object and set of attributes.
        CustomerAccount virtual = CustomerAccountFinder.findOne(CustomerAccountFinder.accountId().eq(600));

        MutableList<Object> virtualAttrs
                = this.getAttributeValuesForObjects(virtual,
                            CustomerAccountFinder.customerId(),
                            CustomerAccountFinder.accountType(),
                            CustomerAccountFinder.accountName());

        Verify.assertListsEqual(FastList.newListWith(999, "Virtual", "No customer Account"), virtualAttrs);
    }




//-------------- Question 14 ------------------------------------------------------
// Given a Mithra object, and a list of pairs of Attribute with value,
// apply the values to the appropriate attribute of the Mithra object.
// Do not use Reflection.

    public void applyValues(MithraObject mithraObject, MutableList<Pair<Attribute, Object>> attrValuePairs)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ14()
    {
        Person person10 = PersonFinder.findOne(PersonFinder.personId().eq(10));
        this.applyValues(person10,
                FastList.newListWith(
                        Tuples.<Attribute, Object>pair(PersonFinder.country(), "FR"),
                        Tuples.<Attribute, Object>pair(PersonFinder.name(), "Billy Bob"),
                        Tuples.<Attribute, Object>pair(PersonFinder.age(), 62),
                        Tuples.<Attribute, Object>pair(PersonFinder.country(), "UK")));
        Assert.assertEquals(10, person10.getPersonId());
        Assert.assertEquals("Billy Bob", person10.getName());
        Assert.assertEquals("UK", person10.getCountry());
        Assert.assertEquals(62, person10.getAge());


        CustomerAccount virtual = new CustomerAccount();
        this.applyValues(virtual,
                FastList.newListWith(
                        Tuples.<Attribute, Object>pair(CustomerAccountFinder.customerId(), 42),
                        Tuples.<Attribute, Object>pair(CustomerAccountFinder.accountType(), "Virtual"),
                        Tuples.<Attribute, Object>pair(CustomerAccountFinder.accountName(), "Rainy Day")));
        Assert.assertEquals(0, virtual.getAccountId());
        Assert.assertEquals(42, virtual.getCustomerId());
        Assert.assertEquals("Rainy Day", virtual.getAccountName());
        Assert.assertEquals("Virtual", virtual.getAccountType());
    }




//-------------- Question 15 ------------------------------------------------------
// Given a Mithra finder, return a list of all attributes marked with
// a "classificationType" property with the value type requested.
// Hint:  finder.getPersistentAttributes() will be very useful here.

    public MutableSet<Attribute> findAttributesClassifiedAs(
            RelatedFinder finder,
            final String classificationTypeValue)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ15()
    {
        MutableSet<Attribute> wholeNumberAttributes
                = this.findAttributesClassifiedAs(AllTypesFinder.getFinderInstance(), "wholeNumber");
        Verify.assertSetsEqual(UnifiedSet.newSetWith("byteValue", "shortValue", "intValue", "longValue"),
                wholeNumberAttributes.collect(ATTRIBUTE_TO_NAME_SELECTOR));

        MutableSet<Attribute> floatingPointNumberAttributes
                = this.findAttributesClassifiedAs(AllTypesFinder.getFinderInstance(), "floatingPointNumber");
        Verify.assertSetsEqual(UnifiedSet.newSetWith("floatValue", "doubleValue"),
                floatingPointNumberAttributes.collect(ATTRIBUTE_TO_NAME_SELECTOR));

        Verify.assertEmpty(this.findAttributesClassifiedAs(AllTypesFinder.getFinderInstance(), "imaginaryNumber"));
    }




//-------------- Question 16 ------------------------------------------------------
// Add new attribute properties to AllTypes.xml as follows:
// Attribute Name    Key    Value
// intValue          myId   124
// stringValue       myId   237
// booleanValue      myId   874
// doubleValue       myId   765
// Note:  All the Values are integers.
// Note:  Don't forget that you'll need to regenerate and compile since
//        you changed the XML file!

    @Test
    public void testQ16()
    {
        Assert.assertEquals(Integer.valueOf(124), AllTypesFinder.intValue().getProperty("myId"));
        Assert.assertEquals(Integer.valueOf(237), AllTypesFinder.stringValue().getProperty("myId"));
        Assert.assertEquals(Integer.valueOf(874), AllTypesFinder.booleanValue().getProperty("myId"));
        Assert.assertEquals(Integer.valueOf(765), AllTypesFinder.doubleValue().getProperty("myId"));
    }




//-------------- Question 17 ------------------------------------------------------
// Using the Mithra metadata, create a map of the Integer myId values to Mithra Attribute,
// for the AllTypes Mithra object.

    public MutableMap<Integer, Attribute> getMyIdToAttributeMap()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ17()
    {
        MutableMap<Integer, Attribute> actualMap = this.getMyIdToAttributeMap();

        MutableMap<Integer, Attribute> expectedMap = UnifiedMap.newMapWith(
                Tuples.<Integer, Attribute>pair(Integer.valueOf(124), AllTypesFinder.intValue()),
                Tuples.<Integer, Attribute>pair(Integer.valueOf(237), AllTypesFinder.stringValue()),
                Tuples.<Integer, Attribute>pair(Integer.valueOf(874), AllTypesFinder.booleanValue()),
                Tuples.<Integer, Attribute>pair(Integer.valueOf(765), AllTypesFinder.doubleValue()));

        Verify.assertMapsEqual(expectedMap, actualMap);
    }




//-------------- Question 18 ------------------------------------------------------
// Imagine that you have some external system that sends you changes to a Mithra object
// as a map of myId values to newValue.
// Implement the method that takes the map of changes,
// and applies them to the given Mithra object.
// Hint:  You can use the answer to the previous question to make this easy to implement.

    public void applyChanges(final MithraObject mithraObject, MutableMap<Integer, Object> changes)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ18()
    {
        AllTypes allTypes = new AllTypes();

        // Test no changes
        this.applyChanges(allTypes, UnifiedMap.<Integer, Object>newMap());

        Assert.assertEquals(0, allTypes.getIntValue());
        Assert.assertNull(allTypes.getStringValue());
        Assert.assertFalse(allTypes.isBooleanValue());
        Assert.assertEquals(0.0, allTypes.getDoubleValue(), 0.0);


        MutableMap<Integer, Object> changes = UnifiedMap.newMapWith(
                Tuples.<Integer, Object>pair(124, 65536),  // int
                Tuples.<Integer, Object>pair(237, "Charlie Croker"), // String
                Tuples.<Integer, Object>pair(874, true),  // boolean
                Tuples.<Integer, Object>pair(765, 1.2358));  // double
        this.applyChanges(allTypes, changes);

        Assert.assertEquals(65536, allTypes.getIntValue());
        Assert.assertEquals("Charlie Croker", allTypes.getStringValue());
        Assert.assertTrue(allTypes.isBooleanValue());
        Assert.assertEquals(1.2358, allTypes.getDoubleValue(), 0.0);
    }




//---------------------------------------------------------------------------------
// Convenience methods and selectors to help make testing easier!

    private static final Function<Attribute,String> ATTRIBUTE_TO_NAME_SELECTOR
            = new Function<Attribute, String>()
    {
        public String valueOf(Attribute attribute)
        {
            return attribute.getAttributeName();
        }
    };
}
