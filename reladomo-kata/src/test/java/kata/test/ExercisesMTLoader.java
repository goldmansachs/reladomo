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
import java.util.List;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.function.primitive.IntFunction;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.factory.Comparators;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.test.Verify;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.mtloader.AbortException;
import com.gs.fw.common.mithra.mtloader.InputLoader;
import com.gs.fw.common.mithra.mtloader.MatcherThread;
import com.gs.fw.common.mithra.mtloader.PlainInputThread;
import com.gs.fw.common.mithra.util.SingleQueueExecutor;
import kata.domain.Employee;
import kata.domain.EmployeeFinder;
import kata.domain.EmployeeList;
import kata.util.TimestampProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExercisesMTLoader
        extends AbstractMithraTest
{
    private static final int NUM_THREADS = 3;
    private static final int BATCH_SIZE = 1;
    private SingleQueueExecutor executor;
    private MutableList<Employee> fileList;

    @Override
    protected String[] getTestDataFilenames()
    {
        return new String[]{"test/data_Employee.txt"}; // Look here to see the data for this test
    }

//------------------------ Question 1 --------------------------------------------------------
// Complete the implementation of InputDataLoader. Look at fileList

    public InputLoader getEmployeeDataInputLoader()
    {
        return new InputDataLoader();
    }

    @Test
    public void testQ1()
    {
        InputLoader employeeDataInputLoader = this.getEmployeeDataInputLoader();

        Verify.assertSize(9, employeeDataInputLoader.getNextParsedObjectList());
    }

//------------------------ Question 2 --------------------------------------------------------
// Get all current Employee data from database

    public EmployeeList getEmployeeDataFromDb()
    {
        Assert.fail("Implement this functionality as a part of Q2");
        return null;
    }

    @Test
    public void testQ2()
    {
        EmployeeList employeeDataFromDb = this.getEmployeeDataFromDb();

        employeeDataFromDb.size();
        Verify.assertIterableSize(5, employeeDataFromDb);
    }

//------------------------ Question 3 --------------------------------------------------------
// You have file data from Q1, database data from Q2. You have matcherThread created for you.
// Reconcile the data and load it.
// Close out database rows which are not in file.
// Insert new rows which are in file but not in database.
// Update necessary rows for change in data between file and database.

    public void loadData()
    {
        Assert.fail("Implement this functionality as a part of Q3");
    }

    @Test
    public void testQ3()
    {
        this.loadData();

        EmployeeList updatedEmployeeDataInDb = this.getEmployeeDataFromDb();
        updatedEmployeeDataInDb.setOrderBy(EmployeeFinder.employeeId().ascendingOrderBy());

        this.fileList.sortThisBy(new Function<Employee, Integer>()
        {
            @Override
            public Integer valueOf(Employee employee)
            {
                return employee.getEmployeeId();
            }
        });

        ExercisesMTLoader.assertEmployeeListsEqual(this.fileList, updatedEmployeeDataInDb);

        Employee employeeId2 = EmployeeFinder.findByPrimaryKey(2, TimestampProvider.getInfinityDate(), TimestampProvider.getInfinityDate());

        Assert.assertEquals("Bob", employeeId2.getFirstName());
        Assert.assertEquals("Smith", employeeId2.getLastName());
    }

//------------------------ Question 4 --------------------------------------------------------
// You have file data from Q1, database data from Q2. You have matcherThread created for you.
// Reconcile the data and load it.
// Make sure you do not compare the Last Names of Employees.
// Close out database rows which are not in file.
// Insert new rows which are in file but not in database.
// Update necessary rows for change in data between file and database.

    public void loadDataWithoutComparingLastName()
    {
        Assert.fail("Implement this functionality as a part of Q4");
    }

    @Test
    public void testQ4()
    {
        this.loadDataWithoutComparingLastName();

        EmployeeList updatedEmployeeDataInDb = this.getEmployeeDataFromDb();
        updatedEmployeeDataInDb.setOrderBy(EmployeeFinder.employeeId().ascendingOrderBy());

        Employee employeeId2 = EmployeeFinder.findByPrimaryKey(2, TimestampProvider.getInfinityDate(), TimestampProvider.getInfinityDate());

        Assert.assertEquals("Bob", employeeId2.getFirstName());
        Assert.assertEquals("Burger", employeeId2.getLastName());
        Assert.assertEquals(Timestamp.valueOf("2009-03-12 00:00:00.0"), employeeId2.getProcessingDateFrom());
    }

//------------------------ Setup --------------------------------------------------------

    @Before
    public void setup()
    {
        this.fileList = getFileList();
        this.executor = new SingleQueueExecutor(
                NUM_THREADS,
                EmployeeFinder.employeeId().ascendingOrderBy(),
                BATCH_SIZE,
                EmployeeFinder.getFinderInstance(),
                BATCH_SIZE
        );
    }

    private static MutableList<Employee> getFileList()
    {
        Timestamp currentTimestamp = Timestamp.valueOf("2015-02-01 00:00:00.0");

        Employee mary = new Employee(currentTimestamp, 1, "Mary", "Lamb", 26);
        Employee bob = new Employee(currentTimestamp, 2, "Bob", "Smith", 29);
        Employee ted = new Employee(currentTimestamp, 3, "Ted", "Smith", 33);
        Employee jake = new Employee(currentTimestamp, 4, "Jake", "Snake", 42);
        Employee barry = new Employee(currentTimestamp, 5, "Barry", "Bird", 28);
        Employee terry = new Employee(currentTimestamp, 6, "Terry", "Chase", 19);
        Employee harry = new Employee(currentTimestamp, 7, "Harry", "White", 22);
        Employee john = new Employee(currentTimestamp, 8, "John", "Doe", 45);
        Employee jane = new Employee(currentTimestamp, 9, "Jane", "Wilson", 28);

        return FastList.newListWith(mary, bob, ted, jake, barry, terry, harry, john, jane);
    }

    private class InputDataLoader implements InputLoader
    {
        private boolean firstTime = true;

        @Override
        public List<? extends MithraTransactionalObject> getNextParsedObjectList()
        {
            Assert.fail("Implement this functionality as a part of testQ1");
            return null;
        }

        @Override
        public boolean isFileParsingComplete()
        {
            if (firstTime)
            {
                firstTime = false;
                return false;
            }
            else
            {
                return true;
            }
        }
    }

    private static void assertEmployeeListsEqual(List<Employee> expectedList, List<Employee> actualList)
    {
        Assert.assertEquals(expectedList.size(), actualList.size());
        for (int index = 0; index < actualList.size(); index++)
        {
            Employee eachExpected = expectedList.get(index);
            Employee eachActual = actualList.get(index);
            Assert.assertEquals(eachExpected.getEmployeeId(), eachActual.getEmployeeId());
            Assert.assertEquals(eachExpected.getAge(), eachActual.getAge());
            Assert.assertEquals(eachExpected.getFirstName(), eachActual.getFirstName());
            Assert.assertEquals(eachExpected.getLastName(), eachActual.getLastName());
        }
    }
}