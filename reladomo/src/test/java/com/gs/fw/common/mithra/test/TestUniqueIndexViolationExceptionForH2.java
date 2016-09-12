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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraUniqueIndexViolationException;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.ExchangeRate;
import com.gs.fw.common.mithra.test.domain.ExchangeRateFinder;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.ProductList;

import java.sql.Timestamp;


public class TestUniqueIndexViolationExceptionForH2
extends MithraTestAbstract
{

    public void testUniqueIndexViolationForH2Insert()
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Product product = new Product();
                    product.setProductId(1);
                    product.insert();
                    return null;
                }
            });
        }
        catch(MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: "));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }

    public void testUniqueIndexViolationForH2BatchInsert()
    {
        try
        {
            final ProductList productList = new ProductList();

            // default batch size is 32, so create product objects with 2 batches which should all get inserted successfully
            for(int i=5;i<10;i++)
            {
                Product product = new Product();
                product.setProductId(i);
                productList.add(product);
            }

            // now add elements which violate unique index
            for(int i = 1; i < 5; i++)
            {
                Product product = new Product();
                product.setProductId(i);
                productList.add(product);
            }

            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    productList.insertAll();
                    return null;
                }
            });
        }
        catch(MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 1")
                                                                            && e.getMessage().contains("Primary Key: productId: 9"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }
    
    public void testUniqueIndexViolationForH2Update()
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    ExchangeRate exchRate = new ExchangeRate();
                    exchRate.setAcmapCode(SOURCE_A);
                    exchRate.setCurrency("USD");
                    exchRate.setDate(new Timestamp(createBusinessDate(2004, 9, 30).getTime()));
                    exchRate.setSource(27);

                    exchRate.insert();
                    return null;
                }
            });
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Operation op = ExchangeRateFinder.acmapCode().eq(SOURCE_A);
                    op = op.and(ExchangeRateFinder.currency().eq("USD"));
                    op = op.and(ExchangeRateFinder.date().eq(createBusinessDate(2004, 9, 30)));
                    op = op.and(ExchangeRateFinder.source().eq(27));

                    ExchangeRate exchRate = ExchangeRateFinder.findOne(op);
                    exchRate.setSource(10);
                    return null;
                }
            });
        }
        catch(MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key",
                e.getMessage().contains("Primary Key: acmapCode: 'A' / currency: 'USD' / date: '2004-09-30 18:30:00.000' / source: 10 /"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }
}
