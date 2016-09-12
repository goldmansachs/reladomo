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


import com.gs.fw.common.mithra.test.domain.AltProduct;
import com.gs.fw.common.mithra.test.domain.AltProductFinder;
import com.gs.fw.common.mithra.test.domain.AltSynonym;
import com.gs.fw.common.mithra.test.domain.AltSynonymFinder;
import com.gs.fw.common.mithra.test.domain.AltSynonymList;
import com.gs.fw.common.mithra.test.domain.AltSynonymType;

public class ParametizedRelationshipTest extends MithraTestAbstract
{
    public void testSimpleRelationships()
    {
        final AltProduct product = AltProductFinder.findOne(AltProductFinder.name().eq("Boddingtons"));

        final AltSynonymList synonyms = product.getSynonyms();
        synonyms.setOrderBy(AltSynonymFinder.synonymTypeId().ascendingOrderBy());
        assertEquals(3, synonyms.size());
        assertEquals("BODS", synonyms.get(0).getValue());

        final AltSynonymType synonymType = synonyms.get(0).getSynonymType();
        assertEquals("VALOREN", synonymType.getName());
    }

    // Demonstrates that the "finder" version of the test passes -- it's just the getter.
    public void testParametizedRelationshipViaFinder()
    {
        final AltProduct product = AltProductFinder.findOne(AltProductFinder.synonymByType("SEDOL").value().eq("8675309"));

        assertEquals("Boddingtons", product.getName());
    }

    // Demonstrates that the "manually-fixed" (getSynonymByTypeFixed()) version of the test passes.
    // getSynonymByTypeFixed() is what the corrected generation should produce.
    public void testParametrizedRelationshipFixed()
    {
        final AltProduct product = AltProductFinder.findOne(AltProductFinder.name().eq("Boddingtons"));

        final AltSynonym synonym = product.getSynonymByTypeFixed("SEDOL");
        assertEquals("8675309", synonym.getValue());
        assertEquals("SEDOL", synonym.getSynonymType().getName());
    }

    public void testParametrizedRelationship()
    {
        final AltProduct product = AltProductFinder.findOne(AltProductFinder.name().eq("Boddingtons"));

        final AltSynonym synonym = product.getSynonymByType("SEDOL");
        assertEquals("8675309", synonym.getValue());
        assertEquals("SEDOL", synonym.getSynonymType().getName());
    }
}
