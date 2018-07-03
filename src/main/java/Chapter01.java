import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: ZouTai
 * @date: 2018/6/27
 * @description: 第一章:初识redis-例子-对文章进行投票
 */
public class Chapter01 {
    /**
     * 一周总秒数；一次支持加432分；一页包含25篇文章
     */
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static final void main(String[] args) {
        new Chapter01().run();
    }

    public void run() {
        // 切换到指定的数据库，数据库索引号 index 用数字值指定，以 0 作为起始索引值。
        // 默认使用 0 号数据库。
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        // 1、发表新文章
        String articleId = postArticle(
                conn, "username", "A title", "http://www.google.com");
        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");

        // 2、输出所有文章
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();

        // 3、对文章进行投票
        articleVote(conn, "other_user", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes"); // 获取文章投票数
        System.out.println("We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) > 1;

        // 4、输出所有的文章数据
        System.out.println("The currently highest-scoring articles are:");
        List<Map<String, String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;

        // 5、文章分组
        addGroups(conn, articleId, new String[]{"new-group"});
        System.out.println("We added the article to a new group, other articles include:");
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    /**
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return 发表新的文章
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
        String articleId = String.valueOf(conn.incr("article:")); // 即文章id从1依次递增（文章表）
        String voted = "voted:" + articleId;
        conn.sadd(voted, user); // (投票表)
        conn.expire(voted, ONE_WEEK_IN_SECONDS); //（设置过期时间为一周）

        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String, String> articleData = new HashMap<String, String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));// （文章发表时间）
        articleData.put("votes", "1");
        conn.hmset(article, articleData);// （文章详细数据表）

        // 分数score用于对文章进行排序，排序由发表时间和投票数两个因素决定，
        // 默认一篇文章一天投票200次，则每一次投票相当于增加432秒的分数
        // 初始分数为发表时的当前时间（now+432）
        conn.zadd("score:", now + VOTE_SCORE, article); // （分数表-用于排序）
        conn.zadd("time:", now, article); // (时间表-用于通过发表时间查找)

        return articleId;
    }

    public void articleVote(Jedis conn, String user, String article) {
        // 判断投票文章是否过期（一个星期），过期不能投票
        // 判断方法：当前时间-过期时间>文章发表时间(过期)
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        if (conn.zscore("time:", article) < cutoff) {
            return;
        }

        String articleId = article.substring(article.indexOf(':') + 1);
        if (conn.sadd("voted:" + articleId, user) == 1) { // 一个人只能投一次票，投过票会返回0
            conn.zincrby("score:", VOTE_SCORE, article); // 增加分数
            conn.hincrBy(article, "votes", 1); // 增加文章投票数
        }
    }


    public List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    public List<Map<String, String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        Set<String> ids = conn.zrevrange(order, start, end);
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        for (String id : ids) {
            Map<String, String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }

        return articles;
    }

    /**
     * 文章分类/分组
      */
    public void addGroups(Jedis conn, String articleId, String[] toAdd) {
        String article = "article:" + articleId;
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }

    /**
     *
     * @param conn
     * @param group
     * @param page
     * @param order
     * @return
     * 1、zinterstore交集；
     *  (Article:id)与(Article:id-score)交集可以对分组文章进行排序
     *  即("group:" + group, order)-->("group:new-group", "score:")
     * 2、缓存：
     *  zinterstore太耗时，所以使用临时表，进行缓存（表key）
     */
    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        String key = order + group;
        if (!conn.exists(key)) { // 先判断是否有缓存
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);
            conn.expire(key, 60); // 缓存1分钟，一分钟后删除表
        }
        return getArticles(conn, page, key);
    }

    private void printArticles(List<Map<String, String>> articles) {
        for (Map<String, String> article : articles) {
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String, String> entry : article.entrySet()) {
                if (entry.getKey().equals("id")) {
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
