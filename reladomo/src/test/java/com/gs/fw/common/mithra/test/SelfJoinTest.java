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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.FileDirectory;
import com.gs.fw.common.mithra.test.domain.FileDirectoryFinder;
import com.gs.fw.common.mithra.test.domain.FileDirectoryList;
import com.gs.fw.common.mithra.test.domain.TestEodAcctIfPnl;
import com.gs.fw.common.mithra.test.domain.TestEodAcctIfPnlFinder;
import com.gs.fw.common.mithra.test.domain.TestEodAcctIfPnlList;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;



public class SelfJoinTest extends MithraTestAbstract
{
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            FileDirectory.class,
            TestEodAcctIfPnl.class        
        };
    }

    public void testParent() throws Exception
    {
        FileDirectory root = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(1));
        FileDirectoryList children = root.getChildDirectories();
        assertTrue(children.size() > 0);
        for(int i=0; i < children.size(); i++)
        {
            FileDirectory child = children.getFileDirectoryAt(i);
            assertSame(root, child.getParentDirectory());
        }
    }

    /**
     * Test relationship
     * <Relationship name="parentDirectoryWithSameNote"
     *    relatedObject="FileDirectory" cardinality="one-to-one">
     *       this.drive = FileDirectory.drive and
     *       this.parentDirectoryId = FileDirectory.fileDirectoryId and
     *       this.note = FileDirectory.note</Relationship>
     */
    public void testParentWithNoteParent()
    {
        // "PARA4.0" and its parent "projects" have the same note, 'note 1'
        FileDirectory para4 = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("PARA4.0"));
        assertEquals("projects", para4.getParentDirectoryWithSameNote().getName());

        // "src" note is null.  Parent directory is "PARA4.0", which has a note of 'note 1'
        FileDirectory src = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("src"));
        assertNull(src.getParentDirectoryWithSameNote());

        // temp has no note, parent has a note, so should return null
        FileDirectory temp = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("temp"));
        assertNull(temp.getParentDirectoryWithSameNote());

        // both system32 and winnt have null notes, but in SQL two nulls are not equal, system32's parentWithSameNote should be null
        FileDirectory system32 = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("system32"));
        assertNull(system32.getParentDirectoryWithSameNote());
    }

    /**
     * Test relationship
     * <Relationship name="parentDirectoryWithSameOtherDate"
     *    relatedObject="FileDirectory" cardinality="one-to-one">
     *       this.drive = FileDirectory.drive and
     *       this.parentDirectoryId = FileDirectory.fileDirectoryId and
     *       this.someOtherDate = FileDirectory.someOtherDate</Relationship>
     */
    /*
     - commented out 'till dates are generated correctly
    public void testParentWithSameOtherDate() {
        // para4 and its parent projects have the same someOtherDate'
        FileDirectory para4 = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("PARA4.0"));
        FileDirectory projects = para4.getParentDirectoryWithSameOtherDate();
        assertNotNull(projects);

        // src someOtherDate is null.  Parent directory is PARA4.0, which has a someOtherDate'
        FileDirectory src = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("src"));
        FileDirectory shouldBeNull = src.getParentDirectoryWithSameOtherDate();
        assertNull(shouldBeNull);

        // temp has no someOtherDate, parent does have one, so should return null
        FileDirectory temp =  FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("temp"));
        shouldBeNull = temp.getParentDirectoryWithSameOtherDate();
        assertNull(shouldBeNull);

        // both system32 and winnt have null someOtherDate, system32's parent should be winnt
        FileDirectory system32 =  FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("system32"));
        FileDirectory winnt  = system32.getParentDirectoryWithSameOtherDate();
        assertNotNull(winnt);
    }
    */

    /**
     * Test relationship
     * <Relationship name="parentDirectoryWithSameOtherDate"
     *    relatedObject="FileDirectory" cardinality="one-to-one">
     *       this.drive = FileDirectory.drive and
     *       this.parentDirectoryId = FileDirectory.fileDirectoryId and
     *       this.someOtherTimestamp = FileDirectory.someOtherTimestamp</Relationship>
     */
    public void testParentWithSameOtherTimestamp()
    {
        // "PARA4.0" and its parent "projects" have the same someOtherTimestamp
        FileDirectory para4 = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("PARA4.0"));
        assertEquals("projects", para4.getParentDirectoryWithSameOtherTimestamp().getName());

        // src someOtherTimestamp is null.  Parent directory is PARA4.0, which has a someOtherTimestamp
        FileDirectory src = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("src"));
        assertNull(src.getParentDirectoryWithSameOtherTimestamp());

        // temp has no someOtherTimestamp, parent does have one, so should return null
        FileDirectory temp = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("temp"));
        assertNull(temp.getParentDirectoryWithSameOtherTimestamp());

        // both system32 and winnt have null someOtherTimestamp, but in SQL two nulls are not equal, system32's parentWithSameOtherTimestamp should be null
        FileDirectory system32 = FileDirectoryFinder.findOne(FileDirectoryFinder.name().eq("system32"));
        assertNull(system32.getParentDirectoryWithSameOtherTimestamp());
    }

    public void testRootParentHasNoParent()
    {
        FileDirectory root = new FileDirectory();
        root.setFileDirectoryId(666);
        root.setName("ROOT");
        root.setDrive("C");
        Timestamp timestamp = new Timestamp(new Date().getTime());
        root.setCreationDate(timestamp);
        root.setModificationDate(timestamp);
        root.insert();
        FileDirectory leaf = new FileDirectory();
        leaf.setFileDirectoryId(999);
        leaf.setName("Leaf");
        leaf.setDrive("C");
        leaf.setCreationDate(timestamp);
        leaf.setModificationDate(timestamp);
        leaf.setParentDirectoryId(root.getFileDirectoryId());
        leaf.insert();
        leaf = null;
        leaf = FileDirectoryFinder.findOne(
                FileDirectoryFinder.fileDirectoryId().eq(999).and(
                        FileDirectoryFinder.drive().eq("C")));
        assertNotNull(leaf);
        root = null;
        root = leaf.getParentDirectory();
        assertNotNull(root);
        FileDirectory shouldBeNull = root.getParentDirectory();
        assertNull(shouldBeNull);
    }

    public void testChildren() throws Exception
    {
        FileDirectory root = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(1));
        FileDirectory temp = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(2));
        assertSame(temp.getParentDirectory(), root);
        assertTrue(root.getChildDirectories().contains(temp));
        FileDirectory src = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(5));
        assertEquals("src", src.getName());
        assertEquals("PARA4.0", src.getParentDirectory().getName());
        assertEquals("projects", src.getParentDirectory().getParentDirectory().getName());
    }
    
    public void testNoParent()
    {
    	// root has ID of 1
    	FileDirectory root = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(1));
    	FileDirectory shouldBeNull = root.getParentDirectory();
    	assertNull(shouldBeNull);
    }

    public void testSelfJoinWithDualConstant()
    {
        FileDirectory src = FileDirectoryFinder.findOne(FileDirectoryFinder.fileDirectoryId().eq(5));
        FileDirectoryList list = src.getJustChildDirectories();
        assertTrue(list.size() > 0);
        for(int i=0;i<list.size();i++)
        {
            assertEquals(1, list.getFileDirectoryAt(i).getIsDirectory());
        }
    }

    public void testSelfJoinWithDualConstantDeepFetch()
    {
        FileDirectoryList directories = new FileDirectoryList(FileDirectoryFinder.isDirectory().eq(1).and(FileDirectoryFinder.name().startsWith("s")));
        directories.deepFetch(FileDirectoryFinder.justChildDirectories());
        assertTrue(directories.size() > 0);
        for(int i=0;i<directories.size();i++)
        {
            FileDirectoryList justChildDirectories = directories.getFileDirectoryAt(i).getJustChildDirectories();
            for(int j=0;j<justChildDirectories.size();j++)
            {
                assertEquals(1, justChildDirectories.getFileDirectoryAt(j).getIsDirectory());
            }
        }
    }

    public void testDateOnRelatedObject() throws ParseException
    {
        Timestamp date1 = new Timestamp(timestampFormat.parse("2005-09-01 00:00:00").getTime());
        Timestamp date2 = new Timestamp(timestampFormat.parse("2005-09-02 00:00:00").getTime());
        Timestamp updateCutoffTime = new Timestamp(timestampFormat.parse("2005-09-03 00:00:00").getTime());
        Operation priorLtdOp;
        priorLtdOp = TestEodAcctIfPnlFinder.businessDate().eq(date1);
        priorLtdOp = priorLtdOp.and(TestEodAcctIfPnlFinder.updatedBalancesOnOtherDate().processingDateFrom().greaterThan(updateCutoffTime));
        priorLtdOp = priorLtdOp.and(TestEodAcctIfPnlFinder.updatedBalancesOnOtherDate().businessDate().eq(date2));

        TestEodAcctIfPnlList list = new TestEodAcctIfPnlList(priorLtdOp);
        list.forceResolve();
        assertEquals(1, list.size());
    }
}
