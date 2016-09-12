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
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.collections.impl.test.Verify;
import java.sql.Timestamp;
import java.util.Date;
import kata.domain.Task;
import kata.domain.TaskFinder;
import kata.domain.TaskList;
import org.junit.Assert;
import org.junit.Test;

public class ExercisesAuditOnly
        extends AbstractMithraTest
{
    @Override
    protected String[] getTestDataFilenames()
    {
        return new String[] {"test/data_Tasks.txt"}; // Look into this file to see the test data being used
    }



// **** NOTES ****
// (1)
// Different applications can have different conventions to represent the business dates and processing dates.
// In this exercise, we are using:
// * IN_Z<@processingDate, OUT_Z> = @processingDate (see toIsInclusive() in processingDate attribute
// * Infinity date = "9999-12-01 23:59:00.0", a.k.a. kata.util.TimestampProvider.getInfinityDate().
// Look at the definition of the processingDate attribute in Task.xml to see how this is defined/used.
//    
// (2)
// Are you completely unfamiliar with chaining?
// See the Reladomo documentation



//------------------------ Question 1 --------------------------------------------------------
// Get all Tasks

    public TaskList getActiveTasks()
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ1()
    {
        TaskList tasks = this.getActiveTasks();

        Verify.assertSetsEqual(UnifiedSet.newSetWith("Colonize Moon", "Dam River"),
                tasks.asGscList().collect(TaskFinder.name()).toSet());
        Verify.assertSetsEqual(UnifiedSet.newSetWith("In Design", "Restarted"),
                tasks.asGscList().collect(TaskFinder.status()).toSet());
    }




//------------------------ Question 2 --------------------------------------------------------
// Get all tasks for a given date

    public TaskList getTasksAsOf(Date date)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ2()
    {
        Timestamp processingDate = Timestamp.valueOf("1991-08-15 18:30:00.0");

        TaskList tasks = this.getTasksAsOf(processingDate);

        Verify.assertSetsEqual(UnifiedSet.newSetWith("Build Bridge", "Dam River"),
                tasks.asGscList().collect(TaskFinder.name()).toSet());
        Verify.assertSetsEqual(UnifiedSet.newSetWith("Restarted", "Damming"),
                tasks.asGscList().collect(TaskFinder.status()).toSet());
    }




//------------------------ Question 3 --------------------------------------------------------
// Get history for a specific taskId, across all time

    public TaskList getTaskHistory(int taskId)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ3()
    {
        TaskList tasks = this.getTaskHistory(1);

        Verify.assertAllSatisfy(tasks, Predicates.attributeEqual(TaskFinder.name(), "Build Bridge"));

        Verify.assertSetsEqual(UnifiedSet.newSetWith(
                Timestamp.valueOf("1965-01-01 06:00:00.0"),
                Timestamp.valueOf("1967-01-01 11:30:22.0"),
                Timestamp.valueOf("1990-01-01 06:00:00.0")),
            tasks.asGscList().collect(TaskFinder.processingDateFrom()).toSet());
    }




//------------------------ Question 4 --------------------------------------------------------
// End a task.  The task should no longer show up in current query results.

    public void endTask(final String name)
    {
        Assert.fail("Implement this functionality to make the test pass");
    }


    @Test
    public void testQ4()
    {
        int tasksBefore = new TaskList(TaskFinder.all()).count();

        Task damRiver = TaskFinder.findOne(TaskFinder.name().eq("Dam River"));

        this.endTask(damRiver.getName());

        Assert.assertTrue(damRiver.isDeletedOrMarkForDeletion());

        int tasksAfter = new TaskList(TaskFinder.all()).count();

        Assert.assertEquals(tasksBefore - 1, tasksAfter);
    }




//------------------------ Question 5 --------------------------------------------------------
// Modify the status of an existing task.
// Return the modified task.

    public Task updateTaskStatus(final int taskId, final String newStatus)
    {
        Assert.fail("Implement this functionality to make the test pass");
        return null;
    }


    @Test
    public void testQ5()
    {
        Task colonizeMoon = this.updateTaskStatus(3, "Designing Moonbuggy");

        Assert.assertEquals(3, colonizeMoon.getTaskId());
        Assert.assertEquals("Colonize Moon", colonizeMoon.getName());
        Assert.assertEquals("Designing Moonbuggy", colonizeMoon.getStatus());
        Assert.assertTrue(colonizeMoon.getProcessingDateFrom().after(Timestamp.valueOf("2010-01-01 06:00:00.0")));
    }
}

