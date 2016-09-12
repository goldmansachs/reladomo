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

import com.gs.collections.impl.block.factory.Predicates;
import com.gs.collections.impl.test.Verify;
import kata.domain.Person;
import kata.domain.PersonFinder;
import kata.domain.PersonList;
import org.junit.Assert;
import org.junit.Test;

public class ExercisesDetachedObjects
        extends AbstractMithraTest
{
    @Override
    protected String[] getTestDataFilenames()
    {
        return new String[] { "test/data_Person.txt" }; // Look into this file to see the test data being used
    }



//-------------- Question 1 ------------------------------------------------------
// Give a detached object of "Clark Kent" to the [simulated] GUI for editing.

    public void giveClarkKentToGui()
    {
        Assert.fail("Implement this functionality to make the test pass");
        Person clarkKent = null;
        
        this.guiChangesClarkKent(clarkKent);
    }

    private void guiChangesClarkKent(Person person)
    {
        Assert.assertNotNull(person);
        person.setName("Superman");
        person.setAge(28);
    }


    @Test
    public void testQ1()
    {
        this.giveClarkKentToGui();

        Person clarkKent = PersonFinder.findOne(PersonFinder.personId().eq(5));

        Assert.assertEquals("Clark Kent", clarkKent.getName());
        Assert.assertEquals(38, clarkKent.getAge());
    }




//-------------- Question 2 ------------------------------------------------------
// Accept a changed object from the [simulated] GUI, and persist the changes.

    public void persistClarkKentChangesFromGui(Person person)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ2()
    {
        int peopleBefore = new PersonList(PersonFinder.all()).count();

        Person clarkKent = PersonFinder.findOne(PersonFinder.personId().eq(5)).getDetachedCopy();
        clarkKent.setName("Superman");
        clarkKent.setAge(28);

        this.persistClarkKentChangesFromGui(clarkKent);

        Person superman = PersonFinder.findOne(PersonFinder.personId().eq(5));

        Assert.assertEquals("Superman", superman.getName());
        Assert.assertEquals(28, superman.getAge());

        int peopleAfter = new PersonList(PersonFinder.all()).count();
        Assert.assertEquals(peopleBefore, peopleAfter);

        Assert.assertNull(PersonFinder.findOne(PersonFinder.name().eq("Clark Kent")));
    }




//-------------- Question 3 ------------------------------------------------------
// Give an empty object to the [simulated] GUI that the GUI can make changes to,
// then store the new info as a new row in the DB.

    public void simulateNewObjectFromGuiInput()
    {
        Assert.fail("Implement this functionality to make the test pass");

        this.giveEmptyObjectToGui(null);

        // Write something here to persist the data received back from the GUI.
    }

    private void giveEmptyObjectToGui(Person person)
    {
        person.setName("Wonder Woman");
        person.setCountry("USA");
        person.setAge(32);
    }


    @Test
    public void testQ3()
    {
        int peopleBefore = new PersonList(PersonFinder.all()).count();

        this.simulateNewObjectFromGuiInput();

        int peopleAfter = new PersonList(PersonFinder.all()).count();
        Assert.assertEquals(peopleBefore + 1, peopleAfter);

        Person wonderWoman = PersonFinder.findOne(PersonFinder.name().eq("Wonder Woman"));

        Assert.assertEquals("Wonder Woman", wonderWoman.getName());
        Assert.assertEquals(32, wonderWoman.getAge());
    }




//-------------- Question 4 ------------------------------------------------------
// The [simulated] GUI will make changes to the detached object, but now the user want to "undo"
// and get back to the original values...

    public Person undoDetachedChanges(Person person)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ4()
    {
        Person chuckieEgg = PersonFinder.findOne(PersonFinder.personId().eq(12)).getDetachedCopy();
        chuckieEgg.setName("Super Mario");
        chuckieEgg.setAge(4);

        Person undone = this.undoDetachedChanges(chuckieEgg);

        Assert.assertEquals("Chuckie Egg", undone.getName());
        Assert.assertEquals(12, undone.getAge());

        Assert.assertSame(chuckieEgg, undone);
    }




//-------------- Question 5 ------------------------------------------------------
// Make a new clone of the passed person, and persist it.

    private void makePersistedClone(Person person)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ5()
    {
        int peopleBefore = new PersonList(PersonFinder.all()).count();
        int jangosBefore = new PersonList(PersonFinder.name().eq("Jango Fett")).count();
        Assert.assertEquals(1, jangosBefore);

        Person jangoFett = PersonFinder.findOne(PersonFinder.name().eq("Jango Fett"));
        this.makePersistedClone(jangoFett);

        int peopleAfter = new PersonList(PersonFinder.all()).count();
        PersonList jangosAfter = new PersonList(PersonFinder.name().eq("Jango Fett"));

        Assert.assertEquals(peopleBefore + 1, peopleAfter);
        Verify.assertSize(2, jangosAfter);

        Verify.assertAllSatisfy(jangosAfter, Predicates.attributeEqual(PersonFinder.name(), "Jango Fett"));
        Verify.assertAllSatisfy(jangosAfter, Predicates.attributeEqual(PersonFinder.age(), 25));
        Verify.assertAllSatisfy(jangosAfter, Predicates.attributeEqual(PersonFinder.country(), "MA"));
    }
}
