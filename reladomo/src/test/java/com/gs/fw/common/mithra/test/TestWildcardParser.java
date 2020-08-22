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

import junit.framework.TestCase;
import com.gs.fw.common.mithra.util.WildcardParser;

public class TestWildcardParser extends TestCase
{

    public void testPercentFunctionality()
    {
        WildcardParser parser = new WildcardParser("*");

        assertTrue(parser.matches("abcd"));
        assertEquals("%", parser.getSqlLikeExpression());

        parser = new WildcardParser("a*");
        assertTrue(parser.matches("abcd"));
        assertFalse(parser.matches("babcd"));
        assertEquals("a%", parser.getSqlLikeExpression());

        parser = new WildcardParser("*a");
        assertTrue(parser.matches("abcda"));
        assertFalse(parser.matches("babcdab"));
        assertEquals("%a", parser.getSqlLikeExpression());

        parser = new WildcardParser("a**");
        assertTrue(parser.matches("abcda"));
        assertFalse(parser.matches("babcdab"));
        assertEquals("a%", parser.getSqlLikeExpression());

        parser = new WildcardParser("a**x**");
        assertTrue(parser.matches("abcdax"));
        assertTrue(parser.matches("abcdaxasdf"));
        assertFalse(parser.matches("babcdabxqwer"));
        assertEquals("a%x%", parser.getSqlLikeExpression());

        parser = new WildcardParser("a'**x**");
        assertTrue(parser.matches("a*bcdax"));
        assertTrue(parser.matches("a*bcdaxasdf"));
        assertFalse(parser.matches("babcdabxqwer"));
        assertEquals("a*%x%", parser.getSqlLikeExpression());
    }


    public void testUnderscoreFunctionality()
    {
        WildcardParser parser = new WildcardParser("?");

        assertTrue(parser.matches("a"));
        assertFalse(parser.matches("ab"));
        assertEquals("_", parser.getSqlLikeExpression());

        parser = new WildcardParser("a?");
        assertFalse(parser.matches("a"));
        assertTrue(parser.matches("ab"));
        assertFalse(parser.matches("abcd"));
        assertFalse(parser.matches("bab"));
        assertEquals("a_", parser.getSqlLikeExpression());

        parser = new WildcardParser("?a");
        assertFalse(parser.matches("a"));
        assertTrue(parser.matches("ba"));
        assertFalse(parser.matches("bcda"));
        assertFalse(parser.matches("bab"));
        assertEquals("_a", parser.getSqlLikeExpression());

        parser = new WildcardParser("a??");
        assertFalse(parser.matches("a"));
        assertFalse(parser.matches("ab"));
        assertTrue(parser.matches("abc"));
        assertFalse(parser.matches("abcd"));
        assertFalse(parser.matches("babb"));
        assertEquals("a__", parser.getSqlLikeExpression());
    }

    public void testEscapeFunctionality()
    {
        WildcardParser parser = new WildcardParser("'?");

        assertTrue(parser.matches("?"));
        assertFalse(parser.matches("'a"));
        assertEquals("?", parser.getSqlLikeExpression());

        parser = new WildcardParser("'*");
        assertTrue(parser.matches("*"));
        assertFalse(parser.matches("'abcd"));
        assertEquals("*", parser.getSqlLikeExpression());

        parser = new WildcardParser("''*");
        assertTrue(parser.matches("'abcd"));
        assertEquals("'%", parser.getSqlLikeExpression());

        parser = new WildcardParser("a'*");
        assertTrue(parser.matches("a*"));
        assertEquals("a*", parser.getSqlLikeExpression());

        parser  = new WildcardParser("a''*");
        assertTrue(parser.matches("a'"));
        assertTrue(parser.matches("a'eff"));
        assertEquals("a'%", parser.getSqlLikeExpression());
    }

    public void testCombinedWildcards()
    {
        WildcardParser parser = new WildcardParser("a?b*");
        assertTrue(parser.matches("acbdefg"));
        assertTrue(parser.matches("acb"));
        assertFalse(parser.matches("'abcdefg"));

        parser = new WildcardParser("a*b?");
        assertTrue(parser.matches("acdefbg"));
        assertTrue(parser.matches("abc"));
        assertFalse(parser.matches("ab"));

        parser = new WildcardParser("*a?");
        assertTrue(parser.matches("ab"));
        assertTrue(parser.matches("1234ab"));
        assertFalse(parser.matches("1234a"));

        parser = new WildcardParser("?a?");
        assertTrue(parser.matches("1a1"));
        assertFalse(parser.matches("1a11"));
        assertFalse(parser.matches("11a1"));
        assertFalse(parser.matches("1a"));
        assertFalse(parser.matches("a1"));

        parser = new WildcardParser("b?acd?");
        assertTrue(parser.matches("b1acd1"));
        assertFalse(parser.matches("bacd1"));
        assertFalse(parser.matches("b1acd"));
        assertFalse(parser.matches("b11acd11"));

        parser = new WildcardParser("a*b*c*d*e*");
        assertTrue(parser.matches("abcde"));
        assertTrue(parser.matches("aou87290838470912340982bcd1203480193284e"));
        assertTrue(parser.matches("abcdeasdfasdfasdf"));

    }

    public void testMetaCharacterHandling()
    {
        WildcardParser parser = new WildcardParser("=");

        assertTrue(parser.matches("="));
        assertEquals("==", parser.getSqlLikeExpression());

        parser = new WildcardParser(".");
        assertTrue(parser.matches("."));
        assertFalse(parser.matches("a"));
        assertEquals(".", parser.getSqlLikeExpression());

        parser = new WildcardParser("[");
        assertTrue(parser.matches("["));
        assertEquals("=[", parser.getSqlLikeExpression(new char[] { '[' }));

        parser = new WildcardParser("]");
        assertTrue(parser.matches("]"));
        assertEquals("=]", parser.getSqlLikeExpression(new char[] { ']' }));

        parser = new WildcardParser("^");
        assertTrue(parser.matches("^"));
        assertEquals("^", parser.getSqlLikeExpression());

        parser = new WildcardParser("$");
        assertTrue(parser.matches("$"));
        assertEquals("$", parser.getSqlLikeExpression());

        parser = new WildcardParser("+");
        assertTrue(parser.matches("+"));
        assertFalse(parser.matches("abc"));
        assertEquals("+", parser.getSqlLikeExpression());

        parser = new WildcardParser("{");
        assertTrue(parser.matches("{"));
        assertEquals("{", parser.getSqlLikeExpression());

        parser = new WildcardParser("}");
        assertTrue(parser.matches("}"));
        assertEquals("}", parser.getSqlLikeExpression());

        parser = new WildcardParser("|");
        assertTrue(parser.matches("|"));
        assertEquals("|", parser.getSqlLikeExpression());

        parser = new WildcardParser("|.=[]^+${}");
        assertTrue(parser.matches("|.=[]^+${}"));
        assertEquals("|.===[=]^+${}", parser.getSqlLikeExpression(new char[] {'=', '[', ']', '%', '_'}));

        parser = new WildcardParser("a'b");
        assertTrue(parser.matches("ab"));
        assertEquals("ab", parser.getSqlLikeExpression(new char[] {'=', '[', ']', '%', '_'}));

    }
    
    public void testEscapeCharacterHandling()
    {
        WildcardParser parser = new WildcardParser("\'[1-9\']\'[1-9\']");
        assertTrue(parser.matches("25"));
        assertEquals("[1-9][1-9]", parser.getSqlLikeExpression());
    }
}
