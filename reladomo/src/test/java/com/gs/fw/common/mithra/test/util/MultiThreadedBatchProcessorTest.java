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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test.util;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemList;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.OrderStatus;
import com.gs.fw.common.mithra.test.domain.OrderStatusList;
import com.gs.fw.common.mithra.test.domain.Player;
import com.gs.fw.common.mithra.test.domain.PlayerList;
import com.gs.fw.common.mithra.test.domain.Team;
import com.gs.fw.common.mithra.test.domain.TeamFinder;
import com.gs.fw.common.mithra.test.domain.TeamList;
import com.gs.fw.common.mithra.util.MultiThreadedBatchProcessor;
import com.gs.fw.finder.Navigation;
import org.eclipse.collections.impl.factory.Sets;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.sql.Timestamp;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadedBatchProcessorTest extends MithraTestAbstract
{

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        createOrdersAndItems();
        createPlayersAndTeams("A");
        createPlayersAndTeams("B");
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
    }

    public void testNoShards()
    {
        List deepFetches = FastList.newListWith(OrderFinder.items(), OrderFinder.orderStatus());

        OrderConsumer consumer = new OrderConsumer();

        MultiThreadedBatchProcessor<Order, OrderList> mtbp = new MultiThreadedBatchProcessor<Order, OrderList>(
                OrderFinder.getFinderInstance(),
                OrderFinder.orderId().greaterThanEquals(1000),
                (List<Navigation<Order>>) deepFetches,
                consumer,
                null);
        mtbp.setBatchSize(77);
        mtbp.process();
        assertEquals(1100, consumer.count.get());
    }

    public void testShards()
    {
        List deepFetches = FastList.newListWith(TeamFinder.players());
        TeamConsumer teamConsumer = new TeamConsumer();

        Set shards = Sets.mutable.with("A", "B");
        MultiThreadedBatchProcessor<Team, TeamList> mtbp = new MultiThreadedBatchProcessor<Team, TeamList>(
                TeamFinder.getFinderInstance(),
                TeamFinder.teamId().greaterThanEquals(1000),
                (List<Navigation<Team>>) deepFetches,
                teamConsumer,
                shards);
        mtbp.setBatchSize(77);
        mtbp.process();
        assertEquals(1100, teamConsumer.countA.get());
        assertEquals(1100, teamConsumer.countB.get());
    }

    private static class OrderConsumer implements MultiThreadedBatchProcessor.Consumer<Order, OrderList>
    {
        private AtomicInteger count = new AtomicInteger();

        @Override
        public void startConsumption(MultiThreadedBatchProcessor<Order, OrderList> processor)
        {

        }

        @Override
        public void consume(OrderList list) throws Exception
        {
            count.addAndGet(list.size());
            for (Order o : list)
            {
                assertEquals(2, o.getItems().size());
                assertNotNull(o.getOrderStatus());
            }
        }

        @Override
        public void endConsumption(MultiThreadedBatchProcessor<Order, OrderList> processor)
        {

        }
    }

    private static class TeamConsumer implements MultiThreadedBatchProcessor.Consumer<Team, TeamList>
    {
        private AtomicInteger countA = new AtomicInteger();
        private AtomicInteger countB = new AtomicInteger();

        @Override
        public void startConsumption(MultiThreadedBatchProcessor<Team, TeamList> processor)
        {

        }

        @Override
        public void consume(TeamList list) throws Exception
        {
            if (list.getTeamAt(0).getSourceId().equals("A"))
            {
                countA.addAndGet(list.size());

            }
            if (list.getTeamAt(0).getSourceId().equals("B"))
            {
                countB.addAndGet(list.size());

            }
            for (Team team : list)
            {
                PlayerList players = team.getPlayers();
                assertEquals(10, players.size());
                Set<String> sourceIds = Sets.mutable.with();
                for (int i = 0; i < players.size(); i++)
                {
                    sourceIds.add(players.get(i).getSourceId());
                }
                assertEquals("Verify all players are from same source", 1, sourceIds.size());
                String playerSourceId = players.get(0).getSourceId();
                assertEquals("Verify team and players are from same source", playerSourceId, team.getSourceId());
            }
        }

        @Override
        public void endConsumption(MultiThreadedBatchProcessor<Team, TeamList> processor)
        {

        }
    }

    private IntHashSet createOrdersAndItems()
    {
        OrderList orderList = new OrderList();
        for (int i = 0; i < 1100; i++)
        {
            Order order = new Order();
            order.setOrderId(i + 1000);
            order.setDescription("order number " + i);
            order.setUserId(i + 7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setTrackingId("T" + i + 1000);
            orderList.add(order);
        }
        orderList.bulkInsertAll();
        OrderItemList items = new OrderItemList();
        IntHashSet itemIds = new IntHashSet();
        for (int i = 0; i < 1100; i++)
        {
            OrderItem item = new OrderItem();
            item.setOrderId(i + 1000);
            item.setId(i + 1000);
            items.add(item);

            item = new OrderItem();
            item.setOrderId(i + 1000);
            item.setId(i + 3000);
            items.add(item);

            itemIds.add(i + 1000);
            itemIds.add(i + 3000);
        }
        items.bulkInsertAll();
        OrderStatusList statusList = new OrderStatusList();
        for (int i = 0; i < 1100; i++)
        {
            OrderStatus status = new OrderStatus();
            status.setOrderId(i + 1000);
            status.setLastUser("" + i);
            statusList.add(status);
        }
        statusList.bulkInsertAll();
        return itemIds;
    }

    private void createPlayersAndTeams(String sourceId)
    {
        TeamList teams = new TeamList();
        for (int i = 0; i < 1100; i++)
        {
            Team team = new Team();
            team.setTeamId(i + 1000);
            team.setDivisionId(i);
            team.setName(sourceId + i);
            team.setSourceId(sourceId);
            teams.add(team);
        }

        PlayerList players = new PlayerList();
        int playerId = 1000;
        for (int i = 0; i < 1100; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                Player player = new Player();
                player.setId(playerId);
                player.setTeamId(i + 1000);
                player.setName(sourceId + playerId);
                player.setSourceId(sourceId);
                players.add(player);
                playerId++;
            }
        }
        teams.bulkInsertAll();
        players.bulkInsertAll();
    }
}
