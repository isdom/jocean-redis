/**
 *
 */
package org.jocean.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jocean.redis.RedisClient.RedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;
import rx.Observable;
import rx.Observable.Transformer;
import rx.functions.Func1;

/**
 * @author isdom
 *
 */
public class RedisUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RedisUtil.class);

    public static final String REDIS_OK = "OK";

    private RedisUtil() {
        throw new IllegalStateException("No instances!");
    }

    public static Observable<RedisMessage> cmdAuth(final String passwd) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("AUTH", passwd));
    }

    public static Observable<RedisMessage> cmdInfo() {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("INFO"));
    }

    public static Observable<RedisMessage> cmdSelect(final int dbno) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("SELECT", Integer.toString(dbno)));
    }

    public static class SetCommand {

        SetCommand(final String key, final String value) {
            this._key = key;
            this._value = value;
        }

        public SetCommand px(final long milliseconds) {
            this._px = Long.toString(milliseconds);
            return this;
        }

        public SetCommand ex(final long seconds) {
            this._ex = Long.toString(seconds);
            return this;
        }

        public SetCommand xx() {
            this._xx = true;
            return this;
        }

        public SetCommand nx() {
            this._nx = true;
            return this;
        }

        public Observable<RedisMessage> build() {
            final List<String> cmds = new ArrayList<>();
            cmds.add("SET");
            cmds.add(this._key);
            cmds.add(this._value);
            if (null != this._ex) {
                cmds.add("EX");
                cmds.add(this._ex);
            }
            if (null != this._px) {
                cmds.add("PX");
                cmds.add(this._px);
            }
            if (this._nx) {
                cmds.add("NX");
            }
            if (this._xx) {
                cmds.add("XX");
            }
            return Observable.<RedisMessage>just(RedisUtil.strs2array(cmds.toArray(new String[0])));
        }

        private final String _key;
        private final String _value;

        private String _ex = null;
        private String _px = null;
        private boolean _nx = false;
        private boolean _xx = false;
    }

    public static SetCommand cmdSet(final String key, final String value) {
        return new SetCommand(key, value);
    }

    public static Observable<RedisMessage> cmdGet(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("GET", key));
    }

    public static Observable<RedisMessage> cmdGetSet(final String key, final String value) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("GETSET", key, value));
    }

    public static Observable<RedisMessage> cmdMGet(final String ...keys) {
        final String[] cmds = new String[keys.length + 1];
        cmds[0] = "MGET";
        System.arraycopy(keys, 0, cmds, 1, keys.length);
        return Observable.<RedisMessage>just(RedisUtil.strs2array(cmds));
    }

    public static Observable<RedisMessage> cmdAppend(final String key, final String value) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("APPEND", key, value));
    }

    public static Observable<RedisMessage> cmdStrlen(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("STRLEN", key));
    }

    public static Observable<RedisMessage> cmdGetRange(final String key, final int start, final int end) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("GETRANGE", key, Integer.toString(start), Integer.toString(end)));
    }

    public static Observable<RedisMessage> cmdSetRange(final String key, final int offset, final String value) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("SETRANGE", key, Integer.toString(offset), value));
    }

    public static Observable<RedisMessage> cmdDel(final String... keys) {
        final List<String> cmds = new ArrayList<>();
        cmds.add("DEL");
        cmds.addAll(Arrays.asList(keys));
        return Observable.<RedisMessage>just(RedisUtil.strs2array(cmds.toArray(new String[0])));
    }

    public static Observable<RedisMessage> cmdIncr(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("INCR", key));
    }

    public static Observable<RedisMessage> cmdIncrBy(final String key, final long increment) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("INCRBY", key, Long.toString(increment)));
    }

    public static Observable<RedisMessage> cmdDecr(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("DECR", key));
    }

    public static Observable<RedisMessage> cmdDecrBy(final String key, final long decrement) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("DECRBY", key, Long.toString(decrement)));
    }

    public static Observable<RedisMessage> cmdExpire(final String key, final int seconds) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("EXPIRE", key, Integer.toString(seconds)));
    }

    public static Observable<RedisMessage> cmdExpireAt(final String key, final long unixTimestampInSeconds) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("EXPIREAT", key, Long.toString(unixTimestampInSeconds)));
    }

    public static Observable<RedisMessage> cmdExists(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("EXISTS", key));
    }

    public static Observable<RedisMessage> cmdSetBit(final String key, final long offset, final byte bit) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("SETBIT", key, Long.toString(offset), Byte.toString(bit)));
    }

    public static Observable<RedisMessage> cmdGetBit(final String key, final long offset) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("GETBIT", key, Long.toString(offset)));
    }

    public static Observable<RedisMessage> cmdBitCount(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("BITCOUNT", key));
    }

    public static Observable<RedisMessage> cmdBitCount(final String key, final long start, final long end) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("BITCOUNT", key, Long.toString(start), Long.toString(end)));
    }

    public static Observable<RedisMessage> cmdBitPos(final String key, final byte bit) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("BITPOS", key, Byte.toString(bit)));
    }

    public static Observable<RedisMessage> cmdBitPos(final String key, final byte bit, final long start, final long end) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("BITPOS", key, Byte.toString(bit), Long.toString(start), Long.toString(end)));
    }

    public static Observable<RedisMessage> cmdHSet(final String key, final String field, final String value) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HSET", key, field, value));
    }

    public static Observable<RedisMessage> cmdHMSet(final String key, final String...fvs) {
        return Observable.<RedisMessage>just(RedisUtil.addstrs(RedisUtil.strs2array("HMSET", key), fvs));
    }

    public static Observable<RedisMessage> cmdHGet(final String key, final String field) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HGET", key, field));
    }

    public static Observable<RedisMessage> cmdHGetAll(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HGETALL", key));
    }

    public static Observable<RedisMessage> cmdHExists(final String key, final String field) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HEXISTS", key, field));
    }

    public static Observable<RedisMessage> cmdHLen(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HLEN", key));
    }

    public static Observable<RedisMessage> cmdHDel(final String key, final String field) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HDEL", key, field));
    }

    public static Observable<RedisMessage> cmdHKeys(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HKEYS", key));
    }

    public static Observable<RedisMessage> cmdHVals(final String key) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HVALS", key));
    }

    public static Observable<RedisMessage> cmdHIncrBy(final String key, final String field, final long increment) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HINCRBY", key, Long.toString(increment)));
    }

    public static Observable<RedisMessage> cmdHIncrByFloat(final String key, final String field, final float increment) {
        return Observable.<RedisMessage>just(RedisUtil.strs2array("HINCRBYFLOAT", key, Float.toString(increment)));
    }

    public static Observable<RedisMessage> error(final String errorMessage) {
        return Observable.<RedisMessage>error(new RuntimeException(errorMessage));
    }

    public static ArrayRedisMessage strs2array(final String... strs) {
        final List<RedisMessage> children = new ArrayList<>(strs.length);
        for (final String cmd : strs) {
            children.add(str2bulk(cmd));
        }
        return new ArrayRedisMessage(children);
    }

    public static ArrayRedisMessage addstrs(final ArrayRedisMessage msg, final String... strs) {
        for (final String cmd : strs) {
            msg.children().add(str2bulk(cmd));
        }
        return msg;
    }

    public static FullBulkStringRedisMessage str2bulk(final String str) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(str.getBytes(CharsetUtil.UTF_8)));
    }

    public static boolean isOK(final RedisMessage message) {
        if (message instanceof SimpleStringRedisMessage) {
            final SimpleStringRedisMessage simplemsg = (SimpleStringRedisMessage)message;
            return simplemsg.content().equals(REDIS_OK);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("redis resp isn't OK but [{}]", dumpAggregatedRedisMessage(message));
        }
        return false;
    }

    public static boolean isNull(final RedisMessage message) {
        return (message instanceof FullBulkStringRedisMessage) ? ((FullBulkStringRedisMessage)message).isNull() : false;
    }

    public static boolean equalsInteger(final RedisMessage message, final long expect) {
        if (message instanceof IntegerRedisMessage) {
            final IntegerRedisMessage intmsg = (IntegerRedisMessage)message;
            return intmsg.value() == expect;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("redis resp is not IntegerRedisMessage but [{}]", expect, dumpAggregatedRedisMessage(message));
        }
        return false;
    }

    public static Func1<RedisMessage, Boolean> equalsInteger(final long expect) {
        return msg -> equalsInteger(msg, expect);
    }

    public static Func1<RedisMessage, Observable<RedisMessage>> ifOKThenElse(
            final Observable<RedisMessage> thenRequest,
            final Observable<RedisMessage> elseRequest) {
        return resp -> RedisUtil.isOK(resp) ? thenRequest : elseRequest;
    }

    public static Func1<RedisMessage, Observable<RedisMessage>> ifThenElse(
            final Func1<RedisMessage, Boolean> condition,
            final Observable<RedisMessage> thenRequest,
            final Observable<RedisMessage> elseRequest) {
        return resp -> condition.call(resp) ? thenRequest : elseRequest;
    }

    @SafeVarargs
    public static Transformer<? super RedisConnection, ? extends RedisConnection>
        composite(final Transformer<? super RedisConnection, ? extends RedisConnection> ... transformers) {
        return source -> {
                Observable<RedisConnection> transformered = source;
                for (final Transformer<? super RedisConnection, ? extends RedisConnection> t : transformers) {
                    transformered = transformered.compose(t);
                }
                return transformered;
            };
    }

    public static Transformer<? super RedisConnection, ? extends RedisConnection> authRedis(final String passwd) {
        return source->{
                if (null == passwd || (null != passwd && passwd.isEmpty())) {
                    return source;
                } else {
                    return source.flatMap(conn->conn.defineInteraction(cmdAuth(passwd))
                        .map(checkRespAndPushConnection(conn, "Auth Failed"))
                    );
                }
        };
    }

    public static Transformer<? super RedisConnection, ? extends RedisConnection> selectDB(final int dbno) {
        return source->source.flatMap(conn->conn.defineInteraction(cmdSelect(dbno))
            .map(checkRespAndPushConnection(conn, "Select Failed"))
        );
    }

    @SuppressWarnings("finally")
    private static Func1<RedisMessage, ? extends RedisConnection> checkRespAndPushConnection(final RedisConnection conn,
            final String errMsg) {
        return resp->{
                if (LOG.isDebugEnabled()) {
                    LOG.debug("receive redis resp[{}]", resp);
                }
                if (isOK(resp)) {
                    return conn;
                } else {
                    try {
                        // close redis connection first
                        conn.close();
                    } finally {
                        throw new RuntimeException(errMsg);
                    }
                }
            };
    }

    public static Transformer<? super RedisConnection, ? extends RedisMessage>
        interacts(final Observable<RedisMessage> first,
                @SuppressWarnings("unchecked")
                final Func1<RedisMessage, Observable<RedisMessage>>... extras) {
        return getconn->getconn.flatMap(conn -> {
                    Observable<? extends RedisMessage> resp = conn.defineInteraction(first);
                    for (final Func1<RedisMessage, Observable<RedisMessage>> next : extras ) {
                        resp = resp.flatMap(msg->conn.defineInteraction(next.call(msg)));
                    }
                    return resp.doAfterTerminate(conn.closer());
                });
    }

    public static String dumpAggregatedRedisMessage(final RedisMessage msg) {
        final StringBuilder sb = new StringBuilder();
        if (msg instanceof SimpleStringRedisMessage) {
            sb.append(((SimpleStringRedisMessage) msg).content());
        } else if (msg instanceof ErrorRedisMessage) {
            sb.append(((ErrorRedisMessage) msg).content());
        } else if (msg instanceof IntegerRedisMessage) {
            sb.append(((IntegerRedisMessage) msg).value());
        } else if (msg instanceof FullBulkStringRedisMessage) {
            sb.append(getString((FullBulkStringRedisMessage) msg));
        } else if (msg instanceof ArrayRedisMessage) {
            for (final RedisMessage child : ((ArrayRedisMessage) msg).children()) {
                sb.append(child);
                sb.append('\n');
            }
        } else {
            throw new CodecException("unknown message type: " + msg);
        }

        return sb.toString();
    }

    private static String getString(final FullBulkStringRedisMessage msg) {
        if (msg.isNull()) {
            return "(null)";
        }
        return msg.content().toString(CharsetUtil.UTF_8);
    }
}
