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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.collections.impl.test.Verify;
import com.gs.fw.common.mithra.test.MithraRuntimeConfigVerifier;
import java.io.IOException;
import kata.domain.Person;
import kata.domain.PersonFinder;
import kata.domain.PersonList;
import org.junit.Assert;
import org.junit.Test;

public class ExercisesBasicFinder
        extends AbstractMithraTest
{
    @Override
    protected String[] getTestDataFilenames()
    {
        return new String[] {"test/data_Person.txt"}; // Look into this file to see the test data being used
    }




//-------------- Question 1 ------------------------------------------------------
// Get all people.

    public PersonList getAllPeople()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ1()
    {
        Verify.assertSize(16, this.getAllPeople());
    }




//-------------- Question 2 ------------------------------------------------------
// Get a person with a specific name.

    public Person getPersonNamed(String name)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ2()
    {
        Person kentBeck = this.getPersonNamed("Kent Beck");
        Assert.assertEquals("Kent Beck", kentBeck.getName());
        Assert.assertEquals("USA", kentBeck.getCountry());
        Assert.assertEquals(52, kentBeck.getAge());
        Person douglasAdams = this.getPersonNamed("Douglas Adams");
        Assert.assertEquals("Douglas Adams", douglasAdams.getName());
        Assert.assertEquals("UK", douglasAdams.getCountry());
        Assert.assertEquals(42, douglasAdams.getAge());
    }




//-------------- Question 3 ------------------------------------------------------
// Get people of a certain age.

    public PersonList getPeopleAged(int age)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ3()
    {
        PersonList age24 = this.getPeopleAged(24);
        Verify.assertSetsEqual(UnifiedSet.newSetWith("Ada Lovelace", "Hiro Tanaka"),
                age24.asGscList().collect(PersonFinder.name()).toSet());
    }




//-------------- Question 4 ------------------------------------------------------
// Get people older than the given age.

    public PersonList getPeopleOlderThan(int age)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ4()
    {
        PersonList age24 = this.getPeopleOlderThan(42);
        Verify.assertSetsEqual(UnifiedSet.newSetWith("Kent Beck", "Bob Martin", "Yuki Suzuki"),
                age24.asGscList().collect(PersonFinder.name()).toSet());
    }




//-------------- Question 5 ------------------------------------------------------
// Get a person with the given name *and* age.

    public Person getPersonWithNameAndAge(String name, int age)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ5()
    {
        Person clarkKent = this.getPersonWithNameAndAge("Clark Kent", 38);
        Assert.assertEquals("Clark Kent", clarkKent.getName());
        Assert.assertEquals("USA", clarkKent.getCountry());
        Assert.assertEquals(38, clarkKent.getAge());
    }




//-------------- Question 6 ------------------------------------------------------
// Get people in their 30's.

    public PersonList getPeopleInTheirThirties()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ6()
    {
        PersonList peopleIn30s = this.getPeopleInTheirThirties();
        Verify.assertListsEqual(FastList.newListWith("John Smith", "Yuri Clark", "Clark Kent"),
                peopleIn30s.asGscList().collect(PersonFinder.name()));
    }




//-------------- Question 7 ------------------------------------------------------
// Get people from a given country *or* of a given age.

    public PersonList getPeopleFromCountryOrAged(String country, int age)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ7()
    {
        PersonList people = this.getPeopleFromCountryOrAged("UK", 24);
        Verify.assertSize(3, people);
        Verify.assertListsEqual(FastList.newListWith("Hiro Tanaka", "Ada Lovelace", "Douglas Adams"),
                people.asGscList().collect(PersonFinder.name()));
    }




//-------------- Question 8 ------------------------------------------------------
// Get people whose name starts with the given string.
// Make sure you get all of them ...
// Hint:  Use something to lower the case on both the search term, and the Mithra attribute.

    public PersonList getPeopleNameStartsWithCaseInsensitive(String startsWith)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ8()
    {
        PersonList peopleLower = this.getPeopleNameStartsWithCaseInsensitive("de");
        Verify.assertListsEqual(FastList.newListWith("de Rothschild", "Devon Koch"),
                peopleLower.asGscList().collect(PersonFinder.name()));

        PersonList peopleUpper = this.getPeopleNameStartsWithCaseInsensitive("DE");
        Verify.assertListsEqual(FastList.newListWith("de Rothschild", "Devon Koch"),
                peopleUpper.asGscList().collect(PersonFinder.name()));
    }




//-------------- Question 9 ------------------------------------------------------
// Find all people with name LIKE "%LA%".
// Remember to ignore case.
// Remember that Mithra talks in Java terms: What's the Java equivalent of "LIKE"?

    public PersonList getNamedLike(String searchString)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ9()
    {
        PersonList likeLa = this.getNamedLike("lA");
        Verify.assertListsEqual(FastList.newListWith("Yuri Clark", "Clark Kent", "Ada Lovelace", "Douglas Adams"),
                likeLa.asGscList().collect(PersonFinder.name()));
    }




//-------------- Question 10 ------------------------------------------------------
// Find all people matching the wildcard spec: "*a?a*".
// Remember to ignore case.

    public PersonList getNameMatchingSpec(String specString)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ10()
    {
        PersonList likeLa = this.getNameMatchingSpec("*a?a*");
        Verify.assertListsEqual(FastList.newListWith("Hiro Tanaka", "Ada Lovelace", "Douglas Adams"),
                likeLa.asGscList().collect(PersonFinder.name()));
    }



//-------------- Question 11 ------------------------------------------------------
// Find all people who have their personId equal to their age.
// Note: You wouldn't normally match something to a "meaningless" ID,
//       but we've contrived the use-case for this test/kata/example.

    public PersonList getWhereAgeEqualsId()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ11()
    {
        PersonList ageEqualsId = this.getWhereAgeEqualsId();
        Verify.assertListsEqual(FastList.newListWith("Chuckie Egg", "Donkey Kong"),
                ageEqualsId.asGscList().collect(PersonFinder.name()));
    }




//-------------- Question 12 ------------------------------------------------------
// Get a list of all people whose age minus their ID is less than 18.
// Note: You wouldn't normally subtract something from a "meaningless" ID,
//       but we've contrived the use-case for this test/kata/example.

    public PersonList getAgeLessIdLessThan18()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ12()
    {
        PersonList lessThan18 = this.getAgeLessIdLessThan18();
        Verify.assertListsEqual(FastList.newListWith("Yusuke Sato", "Ada Lovelace", "Chuckie Egg", "Donkey Kong", "Baby Finster", "Jango Fett"),
                lessThan18.asGscList().collect(PersonFinder.name()));
    }




//-------------- Auxiliary test for runtimeConfiguration correctness --------------

    @Test
    public void testRuntimeConfiguration() throws IOException
    {
        new MithraRuntimeConfigVerifier("src/main/resources/xml/configuration/test/TestMithraRuntimeConfig.xml").verifyClasses();
    }
}
