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

import com.gs.fw.common.mithra.test.domain.BitemporalOrder;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.ProductFinder;
import com.gs.fw.common.mithra.test.domain.ProductList;
import com.gs.fw.common.mithra.util.SingleQueueExecutor;



public class NonDatedSingleQueueExecutorTest extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            Product.class,
            BitemporalOrder.class,
        };
    }


    private ProductList createNonDatedMithraObj(int size)
    {
        ProductList productList = new ProductList();
        for (int i = 0; i<size; i++)
        {
            Product product = new Product();
            product.setProductId(i);
            product.setProductCode("pCode: "+i);
            product.setProductDescription("Description "+i);
            product.setManufacturerId(100+i);
            product.setDailyProductionRate(0.01F + (i*0.01F));
            productList.add(product);
        }
        return productList;
    }

    private SingleQueueExecutor createSqeForProduct()
    {
        SingleQueueExecutor executor = new SingleQueueExecutor(2, ProductFinder.productId().ascendingOrderBy(), 10, ProductFinder.getFinderInstance(), 10);
        executor.setUseBulkInsert(); // not significant on this unit test.  cannot bcp to h2.
        executor.setMaxRetriesBeforeRequeue(10);

        return executor;
    }

    private void createTestProductsUsingSqe(int size)
    {
        new ProductList(ProductFinder.all()).deleteAll();
        assertEquals(new ProductList(ProductFinder.all()).size(), 0);

        final SingleQueueExecutor executor = createSqeForProduct();
        ProductList products =  createNonDatedMithraObj(size);

        for(Product p: products)
        {
            executor.addForInsert(p);
        }

        executor.waitUntilFinished();
        ProductFinder.clearQueryCache();
    }

    private ProductList getAllProducts()
    {
        ProductFinder.clearQueryCache();
        return new ProductList(ProductFinder.all());
    }

    public void testAddForInsert()
    {
        createTestProductsUsingSqe(212);
        assertEquals(getAllProducts().size(), 212);
    }

    public void testAddForUpdate()
    {
        final SingleQueueExecutor executor = createSqeForProduct();
        createTestProductsUsingSqe(101);
        ProductList products = new ProductList(ProductFinder.all());
        assertEquals(products.size(), 101);

        for (Product p: products)
        {
            if (p.getProductId()%2 == 0)
            {
                Product updated = p.getNonPersistentCopy();
                updated.setProductCode(String.valueOf(p.getProductId()));
                executor.addForUpdate(p,updated);
            }
        }
        executor.waitUntilFinished();
        assertEquals(products.size(), 101);

        products = getAllProducts();
        for (Product p: products)
        {
            if (p.getProductId()%2 == 0)
            {
                assertEquals(Integer.parseInt(p.getProductCode()), p.getProductId());
            }
            else
            {
                assertEquals(p.getProductCode(),  "pCode: " + p.getProductId());
            }
        }
    }

    public void testAddForTermination()
    {
        final SingleQueueExecutor executor = createSqeForProduct();
        createTestProductsUsingSqe(102);
        ProductList products = new ProductList(ProductFinder.all());
        assertEquals(products.size(), 102);

        for (Product p: products )
        {
            if (p.getProductId()%2 == 0)
            {
                executor.addForTermination(p);
            }
        }
        executor.waitUntilFinished();

        products = getAllProducts();
        assertEquals(products.size(), 51);
        for (Product p: products)
        {
            assertTrue( p.getProductId()%2 != 0);
        }
    }
}
