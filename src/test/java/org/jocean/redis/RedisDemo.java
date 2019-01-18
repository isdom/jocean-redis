package org.jocean.redis;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.jocean.redis.internal.DefaultRedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.redis.RedisMessage;

public class RedisDemo {
    private static final Logger LOG =
            LoggerFactory.getLogger(RedisDemo.class);

    public static void main(final String[] args) throws InterruptedException, IOException {
        try(final DefaultRedisClient client = new DefaultRedisClient()) {

            client.setFornew(RedisUtil.composite(
                    RedisUtil.authRedis("passwd"),
                    RedisUtil.selectDB(255)
                    ));

            @SuppressWarnings("unchecked")
            final RedisMessage ret =
            client.getConnection(new InetSocketAddress("localhost", 6379))
            .compose(RedisUtil.interacts(
                RedisUtil.cmdSet("demo_key", "new hello, world! from isdom").nx().build(),
                RedisUtil.ifOKThenElse(
                    RedisUtil.cmdGet("demo_key"),
                    RedisUtil.error("set failed.")
                    ),
                    resp -> RedisUtil.cmdDel("demo_key")
                ))
            .toBlocking().single();

            LOG.info("recv: {}", RedisUtil.dumpAggregatedRedisMessage(ret));
        }
    }
}
