/**
 *
 */
package org.jocean.redis;

import java.net.SocketAddress;

import org.jocean.idiom.EndAware;

import io.netty.handler.codec.redis.RedisMessage;
import rx.Observable;
import rx.functions.Action0;

/**
 * @author isdom
 *
 */
public interface RedisClient extends AutoCloseable {
    @Override
    public void close();

    public interface RedisConnection extends AutoCloseable, EndAware<RedisConnection> {
        public Action0 closer();
        @Override
        public void close();

        public boolean isActive();

        public Observable<? extends RedisMessage> defineInteraction(
                final Observable<? extends RedisMessage> request);
    }

    public Observable<? extends RedisConnection> getConnection(final SocketAddress remoteAddress);

    public Observable<? extends RedisConnection> getConnection();
}
