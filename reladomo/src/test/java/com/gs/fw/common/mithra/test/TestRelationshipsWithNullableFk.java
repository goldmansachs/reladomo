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


import com.gs.fw.common.mithra.test.domain.Firm;
import com.gs.fw.common.mithra.test.domain.FirmRO;
import com.gs.fw.common.mithra.test.domain.FirmROFinder;
import com.gs.fw.common.mithra.test.domain.Person;
import com.gs.fw.common.mithra.test.domain.PersonAddress;
import com.gs.fw.common.mithra.test.domain.PersonAddressRO;
import com.gs.fw.common.mithra.test.domain.PersonRO;
import com.gs.fw.common.mithra.test.domain.PersonROFinder;

public class TestRelationshipsWithNullableFk extends MithraTestAbstract
{

    public TestRelationshipsWithNullableFk(String s)
    {
        super(s);
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Firm.class,
            Person.class,
            PersonAddress.class,
            FirmRO.class,
            PersonRO.class,
            PersonAddressRO.class
        };
    }

    public void testManyToOne()
    {
        Firm firm = new Firm();
        firm.setId(2);
        firm.setLegalName("GS");
        firm.insert();

        Person person = new Person();
        person.setId(2);
        person.setFirm(firm);
        person.insert();

        assertNotNull(person.getFirm());
        assertEquals("GS", person.getFirm().getLegalName());

        person.setFirmIdNull();

        assertNull(person.getFirm());
    }

    public void testOneToOne()
    {
        Person person = new Person();
        person.setId(2);
        person.setFirstName("Lloyd");
        person.setLastName("Blankfein");
        person.insert();

        Firm firm = new Firm();
        firm.setId(2);
        firm.setLegalName("GS");
        firm.setCeo(person);
        firm.insert();

        assertNotNull(firm.getCeo());
        assertEquals("Blankfein", firm.getCeo().getLastName());

        firm.setCeoIdNull();

        assertNull(firm.getCeo());
    }

    public void testOneToOneQualified()
    {
        Person person = new Person();
        person.setId(2);
        person.setAddressId(2);
        person.setFirstName("Lloyd");
        person.setLastName("Blankfein");
        person.insert();

        PersonAddress homeAddress = new PersonAddress();
        homeAddress.setAddressType(null);
        homeAddress.setId(2);
        homeAddress.setStreet("Washington Street");
        homeAddress.setCity("Hoboken");
        homeAddress.insert();

        PersonAddress vacationAddress = new PersonAddress();
        vacationAddress.setAddressType("Vacation");
        vacationAddress.setId(2);
        vacationAddress.setStreet("Corolla Blvd");
        vacationAddress.setCity("Corolla");
        vacationAddress.insert();

        assertNotNull(person.getAddress(null));
        assertNotNull(person.getAddress("Vacation"));
        assertEquals("Hoboken", person.getAddress(null).getCity());
        assertEquals("Corolla", person.getAddress("Vacation").getCity());

        person.setAddressIdNull();

        assertNull(person.getAddress(null));
        assertNull(person.getAddress("Vacation"));
    }


    public void testManyToOneRO()
    {
        PersonRO person = PersonROFinder.findOne(PersonROFinder.id().eq(1));
        assertEquals("Lloyd", person.getFirstName());
        assertNull(person.getFirm());
    }

    public void testOneToOneRO()
    {
        FirmRO firm = FirmROFinder.findOne(FirmROFinder.id().eq(1));
        assertNull(firm.getCeo());
    }

    public void testOneToOneQualifiedRO()
    {
        PersonRO person = PersonROFinder.findOne(PersonROFinder.id().eq(1));
        assertNotNull(person.getAddress(null));
        assertNotNull(person.getAddress("Vacation"));
        assertEquals("Hoboken", person.getAddress(null).getCity());
        assertEquals("Corolla", person.getAddress("Vacation").getCity());
    }
}
