import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: ZouTai
 * @date: 2018/7/3
 * @description: 第4章：数据安全与性能保障
 * @create: 2018-07-03 11:25
 * 不同于关系型数据库的事务，redis数据库事务并不是使用悲观锁，
 * 在执行事务时，1、不会检查相关变量，2、也不会自动执行重试
 * 所以需要开发者自己手动检查变量，手动重试（redis类似于悲观锁，线程不加锁，但是需要检查）
 */
public class Chapter04 {
    public static void main(String[] args) {
        new Chapter04().run();
    }

    private void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testListItem(conn,false);
        testPurchaseItem(conn);
        testBenchMark(conn);
    }

    private void testBenchMark(Jedis conn) {
        benchmarkUpdateToken(conn, 5);
        benchmarkUpdateToken2(conn, 5);
    }

    /**
     * 不使用反射
     * @param conn
     * @param duration
     */
    private void benchmarkUpdateToken2(Jedis conn, int duration) {
        int count = 0;
        long start = System.currentTimeMillis();
        long end = System.currentTimeMillis() + duration * 1000;
        while (start < end) {
            updateToken(conn, "token", "user", "item");
        }

        long delta = System.currentTimeMillis() - start;
        System.out.println(
                "updateToken" + ' ' +
                        count + ' ' +
                        (delta / 1000) + ' ' +
                        (count / (delta / 1000)));


        count = 0;
        start = System.currentTimeMillis();
        end = System.currentTimeMillis() + duration * 1000;
        while (start < end) {
            updateTokenPipeline(conn, "token", "user", "item");
        }

        delta = System.currentTimeMillis() - start;
        System.out.println(
                "updateToken" + ' ' +
                        count + ' ' +
                        (delta / 1000) + ' ' +
                        (count / (delta / 1000)));
    }

    private void benchmarkUpdateToken(Jedis conn, int duration) {
        try {
            // 使用java反射功能，减少函数冗余（可以对比发现）
            @SuppressWarnings("rawtypes")
            Class[] args = new Class[]{
                Jedis.class, String.class, String.class, String.class
            }; // 定义4个函数参数
            Method[] methods = new Method[]{
                this.getClass().getDeclaredMethod("updateToken", args),
                this.getClass().getDeclaredMethod("updateTokenPipeline", args),
            };
            for (Method method : methods) {
                int count = 0;
                long start = System.currentTimeMillis();
                long end = System.currentTimeMillis() + duration * 1000;
                while (start < end) {
                    method.invoke(this, conn, "token", "user", "item");
                }

                long delta = System.currentTimeMillis() - start;
                System.out.println(
                    method.getName() + ' ' +
                    count + ' ' +
                    (delta / 1000) + ' ' +
                    (count / (delta / 1000)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用非事务模式，但是集成提交任务
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    private void updateTokenPipeline(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        Pipeline pipe = conn.pipelined();
        pipe.multi();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);
        if (item != null) {
            pipe.zadd("viewed:" + token, timestamp, item);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
            pipe.zincrby("viewed:", -1, item);
        }
        pipe.exec();
    }

    private void updateToken(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);
        conn.zadd("recent:", timestamp, token);
        if (item != null) {
            conn.zadd("viewed:" + token, timestamp, item);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    /**
     * 测试2：购买
     * @param conn
     */
    private void testPurchaseItem(Jedis conn) {
        testListItem(conn, true);
        // 初始化买家信息
        conn.hset("users:buyerId", "funds", "100");
        conn.hset("users:buyerId", "name", "buyerName");
        Map<String, String> buyersMap = conn.hgetAll("users:buyerId");
        System.out.println("输出卖家信息：");
        for (Map.Entry<String, String> entry : buyersMap.entrySet()) {
            System.out.println(" " + entry.getKey() + " " + entry.getValue());
        }
        boolean result = purchaseItem(conn, "userID1", "Item1", "buyerId", 10);
    }

    /**
     * 购买
     * @param conn
     * @param sellerId
     * @param itemId
     * @param buyerId
     * @param lprice
     * 以下逻辑类似于发布商品
     */
    private boolean purchaseItem(Jedis conn, String sellerId, String itemId, String buyerId, int lprice) {
        String buyer = "users:" + buyerId;
        String seller = "users:" + sellerId;
        Long endTime = System.currentTimeMillis() + 5000;
        String inventory = "inventory:" + buyerId;
        String item = itemId + '.' + sellerId;
        while (System.currentTimeMillis() < endTime) {
            conn.watch("market:", buyer);
            double price = conn.zscore("market:", item);
            double funds = Double.parseDouble(conn.hget(buyer, "funds"));
            if (price != lprice || price > funds) {
                conn.unwatch();
                return false;
            }
            Transaction trans = conn.multi();
            trans.hincrBy(seller, "funds", (long) price);
            trans.hincrBy(buyer, "funds", (long) -price);
            trans.sadd(inventory, itemId);
            trans.zrem("market:", item);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null) {
                continue;
            }
            return true;
        }
        return false;
    }

    /**
     * 测试1：
     * 使用事务检查watch，将商品放到市场上
     * @param conn
     * @param nested
     */
    private void testListItem(Jedis conn, boolean nested) {
        if (!nested){
            System.out.println("\n----- testListItem -----");
        }

        String sellerId = "userID1";
        String item = "Item1";
        conn.sadd("inventory:" + sellerId, item);

        System.out.println("当前卖家拥有的商品有：");
        Set<String> itemSet = conn.smembers("inventory:" + sellerId);
        for (String one : itemSet) {
            System.out.println("  " + one);
        }

        listItem(conn, item, sellerId, 10);

        // 输出商场商品：
        System.out.println("输出商场商品：");
        Set<Tuple> sellingItemSets = conn.zrangeWithScores("market:", 0, -1);
        for (Tuple t : sellingItemSets) {
            System.out.println("  " + t.getElement() + "---" + t.getScore());
        }
    }

    /**
     * 发放商品
     *
     * @param conn
     * @param item
     * @param sellerId
     * @param price
     */
    private boolean listItem(Jedis conn, String item, String sellerId, double price) {
        long endTime = System.currentTimeMillis() + 5000;

        String inventory = "inventory:" + sellerId;
        String itemSellerId = item + "." + sellerId;
        while (System.currentTimeMillis() < endTime) {

            // 1.1 监视表inventory，线程可见；乐观锁
            conn.watch(inventory);
            // 1.2 检查卖家是否有这个商品，若没有，返回false
            if (!conn.sismember(inventory, item)) {
                conn.unwatch();
                return false;
            }
            // 1.3 开启事务流水线
            Transaction transaction = conn.multi();
            transaction.zadd("market:", price, itemSellerId);
            transaction.srem(inventory, item);
            // 1.4 执行流水线操作
            List<Object> results = transaction.exec();

            // 1.5 如果事务执行失败，则重试-继续执行，直到超时
            if (results == null) {
                continue;
            }
            return true;
        }
        return false;
    }
}
