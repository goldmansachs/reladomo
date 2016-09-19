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

package sample.domain;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersonTest
        extends AbstractReladomoTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PersonTest.class);

    @Override
    protected String[] getTestDataFilenames()
    {
        return new String[] {"test_data.txt"}; // Look into this file to see the test data being used
    }

    @Test
    public void testPersonRetrieval()
    {
        Person john = Person.findPersonNamed("John", "Smith");
        Assert.assertEquals("USA", john.getCountry());
    }

    @Test
    public void testPersonCreation()
    {
        Assert.assertEquals(4, PersonFinder.findMany(PersonFinder.all()).size());

        Person.createPerson("Sarah", "Collins", "USA");

        Assert.assertNotNull(PersonFinder.findOne(PersonFinder.firstName().eq("Sarah")));
        Assert.assertEquals("Sarah Collins", PersonFinder.findOne(PersonFinder.firstName().eq("Sarah")).getFullName());
        Assert.assertEquals(5, PersonFinder.findMany(PersonFinder.all()).size());
    }
}
