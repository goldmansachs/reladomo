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
import com.gs.fw.common.mithra.list.InsertAllTransactionalCommand;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.ProductFinder;
import com.gs.fw.common.mithra.test.domain.ProductList;

import java.sql.Connection;
import java.sql.SQLException;

public class TestUniqueIndexViolationExceptionForSybase
extends MithraSybaseTestAbstract
{

    public void testUniqueIndexViolationForSybaseInsert()
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
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 1"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }

    public void testUniqueIndexViolationForSybaseMultiInsert()
    {
        try
        {
            final ProductList productList = new ProductList();

            // default batch size is 32, so create product objects with 2 batches which should all get inserted successfully
            for(int i = 5; i < 69; i++ )
            {
                Product product = new Product();
                product.setProductId(i);
                productList.add(product);
            }

            // now add elements which violate unique index
            for(int i = 1; i < 33; i++)
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
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 1"));
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 31"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }

    public void testUniqueIndexViolationForSybaseBulkInsert()
    {
        final int listSize = 150;
        try
        {
            final ProductList productList = new ProductList();

            for (int i=0; i<listSize; i++)
            {
                Product product = new Product();
                product.setProductId(i);
                productList.add(product);
            }
            TransactionalCommand command = new InsertAllTransactionalCommand(productList, 100);
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(command);
        }
        catch(MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected string",
                e.getMessage().contains("Primary Key: productId: 2"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }
    
    public void testUniqueIndexViolationForSybaseUpdate() throws SQLException
    {
        Connection con = SybaseTestConnectionManager.getInstance().getConnection();
        String dropSql = "if exists (select name from sysindexes where name = 'PROD_DESC' and id=object_id('PRODUCT')) " +
                                                    "drop index PRODUCT.PROD_DESC";
        con.createStatement().execute(dropSql);
        StringBuffer uniqueIndexSql = new StringBuffer("create unique index PROD_DESC on PRODUCT(PROD_DESC)");
        con.createStatement().execute(uniqueIndexSql.toString());
        
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Product product = ProductFinder.findOne(ProductFinder.productId().eq(2));
                    product.setProductDescription("Product 1");
                    return null;
                }
            });
        }
        catch(MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 2"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
        finally
        {
            if (con == null)
            {
                con = SybaseTestConnectionManager.getInstance().getConnection();
            }
            con.createStatement().execute(dropSql);
            con.rollback();
            con.close();
        }
    }    
}
