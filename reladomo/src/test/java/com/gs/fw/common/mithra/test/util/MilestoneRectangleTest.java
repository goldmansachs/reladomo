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

package com.gs.fw.common.mithra.test.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

import com.gs.fw.common.mithra.util.dbextractor.MilestoneRectangle;



/**
 * PhaseSpaceRectangleTest
 */
public class MilestoneRectangleTest extends TestCase
{
    public void testMoh()
    {
        MergeReturn result = merging(r("a", 3, 4, 2, 4), r("b", 2, 6, 1, 4), r("c", 1,5,1,3));
        System.out.println(result);
    }

    public void testBasicScenarios()
    {
        merging().
        returns();
        
        merging(r("a", 1, 5, 10, 50)).
        returns(r("a", 1, 5, 10, 50));
        
        merging(r("a", 1, 5, 10, 50), r("b", 10, 15, 100, 150)).
        returns(r("a", 1, 5, 10, 50), r("b", 10, 15, 100, 150));

        merging(r("a", 1, 5, 10, 50), r("b", 1, 5, 10, 50)).
        returns(r("a", 1, 5, 10, 50));
    }

    public void testEdgeScenarios()
    {
        // merging rectangles that touch on each edge should result in no fragmentation 
        merging(r("a", 5, 10, 50, 100), r("b", 5, 10, 100, 150)).
        returns(r("a", 5, 10, 50, 100), r("b", 5, 10, 100, 150));
        
        merging(r("a", 5, 10, 50, 100), r("b", 10, 15, 50, 100)).
        returns(r("a", 5, 10, 50, 100), r("b", 10, 15, 50, 100));
        
        merging(r("a", 5, 10, 50, 100), r("b", 5, 10, 0, 50)).
        returns(r("a", 5, 10, 50, 100), r("b", 5, 10, 0, 50));
        
        merging(r("a", 5, 10, 50, 100), r("b", 0, 5, 50, 100)).
        returns(r("a", 5, 10, 50, 100), r("b", 0, 5, 50, 100));
    }

    public void testLeftIntersection()
    {
        merging(r("a", 5, 15, 100, 150), r("b", 10, 20, 50, 200)).
        returns(r("a", 5, 15, 100, 150), r("b", 10, 15, 50, 100), r("b", 10, 15, 150, 200), r("b", 15, 20, 50, 200));
        
        merging(r("a", 5, 15, 100, 150), r("b", 10, 20, 100, 150)).
        returns(r("a", 5, 15, 100, 150), r("b", 15, 20, 100, 150));
        
        merging(r("a", 5, 15, 50, 200), r("b", 10, 20, 100, 150)).
        returns(r("a", 5, 15, 50, 200), r("b", 15, 20, 100, 150));
    }

    public void testBottomIntersection()
    {
        merging(r("a", 10, 15, 50, 150), r("b", 5, 20, 100, 200)).
        returns(r("a", 10, 15, 50, 150), r("b", 5, 10, 100, 200), r("b", 10, 15, 150, 200), r("b", 15, 20, 100, 200));
        
        merging(r("a", 5, 20, 50, 150), r("b", 5, 20, 100, 200)).
        returns(r("a", 5, 20, 50, 150), r("b", 5, 20, 150, 200));
        
        merging(r("a", 5, 20, 50, 150), r("b", 10, 15, 100, 200)).
        returns(r("a", 5, 20, 50, 150), r("b", 10, 15, 150, 200));
    }

    public void testTopIntersection()
    {
        merging(r("a", 10, 15, 100, 200), r("b", 5, 20, 50, 150)).
        returns(r("a", 10, 15, 100, 200), r("b", 5, 10, 50, 150), r("b", 10, 15, 50, 100), r("b", 15, 20, 50, 150));
        
        merging(r("a", 5, 20, 100, 200), r("b", 5, 20, 50, 150)).
        returns(r("a", 5, 20, 100, 200), r("b", 5, 20, 50, 100));
        
        merging(r("a", 5, 20, 100, 200), r("b", 10, 15, 50, 150)).
        returns(r("a", 5, 20, 100, 200), r("b", 10, 15, 50, 100));
    }

    public void testRightIntersection()
    {
        merging(r("a", 10, 20, 100, 150), r("b", 5, 15, 50, 200)).
        returns(r("a", 10, 20, 100, 150), r("b", 5, 10, 50, 200), r("b", 10, 15, 50, 100), r("b", 10, 15, 150, 200));
        
        merging(r("a", 10, 20, 50, 200), r("b", 5, 15, 50, 200)).
        returns(r("a", 10, 20, 50, 200), r("b", 5, 10, 50, 200));
        
        merging(r("a", 10, 20, 50, 200), r("b", 5, 15, 100, 150)).
        returns(r("a", 10, 20, 50, 200), r("b", 5, 10, 100, 150));
    }

    public void testEnclosure()
    {
        merging(r("a", 5, 20, 50, 200), r("b", 10, 15, 100, 150)).
        returns(r("a", 5, 20, 50, 200));
        
        merging(r("a", 10, 15, 100, 150), r("b", 5, 20, 50, 200)).
        returns(r("a", 10, 15, 100, 150), r("b", 5, 10, 50, 200), r("b", 10, 15, 50, 100), r("b", 10, 15, 150, 200), r("b", 15, 20, 50, 200));
    }

    public void testPrecedence()
    {
        merging(r("a", 5, 15, 50, 150), r("b", 10, 20, 100, 200), r("c", 18, 23, 80, 130)).
        returns(r("a", 5, 15, 50, 150), r("b", 10, 15, 150, 200), r("b", 15, 20, 100, 200), r("c", 18, 20, 80, 100), r("c", 20, 23, 80, 130));
        
        merging(r("a", 5, 15, 50, 150), r("c", 18, 23, 80, 130), r("b", 10, 20, 100, 200)).
        returns(r("a", 5, 15, 50, 150), r("c", 18, 23, 80, 130), r("b", 10, 15, 150, 200), r("b", 15, 18, 100, 200), r("b", 18, 20, 130, 200));
    }

    public void testDeleteDuplicates()
    {
        merging(r("a", 1, 2, 3, 4), r("b", 1, 2, 3, 4)).
        returns(r("a", 1, 2, 3, 4));
    }

    public void testSingleDimension()
    {
        merging(r("a", 5, 10), r("b", 5, 10)).
        returns(r("a", 5, 10));

        merging(r("a", 5, 10), r("b", 7, 10)).
        returns(r("a", 5, 10));

        merging(r("a", 5, 10), r("b", 7, 12)).
        returns(r("a", 5, 10), r("b", 10, 12));

        merging(r("a", 5, 10), r("b", 3, 13)).
        returns(r("a", 5, 10), r("b", 3, 5), r("b", 10, 13));
        
        // edge scenarios
        merging(r("a", 5, 10), r("b", 10, 15)).
        returns(r("a", 5, 10), r("b", 10, 15));

        merging(r("a", 5, 10), r("b", 0, 5)).
        returns(r("a", 5, 10), r("b", 0, 5));
    }
    
    public void testIntersects()
    {
        assertTrue(r("a").intersects(r("a")));
        assertTrue(r("a").intersects(r("b")));

        assertFalse(r("a", 5, 10).intersects(r("b", 15, 20)));
        assertFalse(r("a", 5, 10).intersects(r("b", 10, 15)));
        assertFalse(r("a", 5, 10).intersects(r("b", 0, 5)));
        assertTrue(r("a", 5, 10).intersects(r("b", 3, 7)));
        assertTrue(r("a", 5, 10).intersects(r("b", 7, 12)));
        assertTrue(r("a", 5, 10).intersects(r("b", 3, 12)));


        assertFalse(r("a", 5, 10, 50, 100).intersects(r("b", 15, 20, 50, 100)));
        assertFalse(r("a", 5, 10).intersects(r("b", 10, 15)));
        assertFalse(r("a", 5, 10).intersects(r("b", 0, 5)));
        assertTrue(r("a", 5, 10).intersects(r("b", 3, 7)));
        assertTrue(r("a", 5, 10).intersects(r("b", 7, 12)));
        assertTrue(r("a", 5, 10).intersects(r("b", 3, 12)));
    }

    private static MilestoneRectangle r(Object data)
    {
        return r(data, -1, -1);
    }

    private static MilestoneRectangle r(Object data, long from, long thru)
    {
        return r(data, from, thru, -1, -1);
    }

    private static MilestoneRectangle r(Object data, long from, long thru, long in, long out)
    {
        return new MilestoneRectangle(data, from, thru, in, out);
    }

    private MergeReturn merging(MilestoneRectangle... input)
    {
        return new MergeReturn(input);
    }

    private static class MergeReturn
    {
        private final List<MilestoneRectangle> input;

        private MergeReturn(MilestoneRectangle[] input)
        {
            this.input = Arrays.asList(input);
        }

        private void returns(MilestoneRectangle... expected)
        {
            List<MilestoneRectangle> actual = MilestoneRectangle.merge(this.input);
            Collections.reverse(actual);
            assertEquals(Arrays.asList(expected), actual);
        }
    }
}
