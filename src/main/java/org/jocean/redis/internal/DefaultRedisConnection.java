/**
 *
 */
package org.jocean.redis.internal;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.jocean.http.CloseException;
import org.jocean.http.TransportException;
import org.jocean.http.util.Nettys;
import org.jocean.idiom.EndAwareSupport;
import org.jocean.idiom.ExceptionUtils;
import org.jocean.idiom.InterfaceSelector;
import org.jocean.redis.RedisClient.RedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.ActionN;
import rx.subscriptions.Subscriptions;

/**
 * @author isdom
 *
 */
class DefaultRedisConnection implements RedisConnection, Comparable<DefaultRedisConnection>  {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultRedisConnection.class);

    private final InterfaceSelector _selector = new InterfaceSelector();

    @SafeVarargs
    DefaultRedisConnection(final Channel channel, final Action1<RedisConnection> ... onEnds) {

        this._endSupport = new EndAwareSupport<RedisConnection>(this._selector);

        this._channel = channel;
        this._op = this._selector.build(Op.class, OP_ACTIVE, OP_UNACTIVE);

        Nettys.applyToChannel(onEnd(),
                channel,
                RedisHandlers.ON_EXCEPTION_CAUGHT,
                (Action1<Throwable>)cause->fireClosed(cause));

        Nettys.applyToChannel(onEnd(),
                channel,
                RedisHandlers.ON_CHANNEL_INACTIVE,
                (Action0)()->fireClosed(new TransportException("channelInactive of " + channel)));

        for (final Action1<RedisConnection> onend : onEnds) {
            doOnEnd(onend);
        }
        if (!channel.isActive()) {
            fireClosed(new TransportException("channelInactive of " + channel));
        }
    }

    private final Op _op;
    private final Channel _channel;
    private final EndAwareSupport<RedisConnection> _endSupport;

    @Override
    public void close() {
        fireClosed(new CloseException());
    }

    @Override
    public Action0 closer() {
        return ()->close();
    }

    Channel channel() {
        return this._channel;
    }

    @Override
    public Action1<Action0> onEnd() {
        return  this._endSupport.onEnd(this);
    }

    @Override
    public Action1<Action1<RedisConnection>> onEndOf() {
        return  this._endSupport.onEndOf(this);
    }

    @Override
    public Action0 doOnEnd(final Action0 onend) {
        return this._endSupport.doOnEnd(this, onend);
    }

    @Override
    public Action0 doOnEnd(final Action1<RedisConnection> onend) {
        return this._endSupport.doOnEnd(this, onend);
    }

    boolean isTransacting() {
        return 1 == transactingUpdater.get(this);
    }

    @Override
    public Observable<? extends RedisMessage> defineInteraction(final Observable<? extends RedisMessage> request) {
        return Observable.unsafeCreate(subscriber->this._op.subscribeResponse(this, request, subscriber));
    }

    @Override
    public boolean isActive() {
        return this._selector.isActive();
    }

    private boolean holdInboundSubscriber(final Subscriber<?> subscriber) {
        return inboundSubscriberUpdater.compareAndSet(this, null, subscriber);
    }

    private boolean unholdInboundSubscriber(final Subscriber<?> subscriber) {
        return inboundSubscriberUpdater.compareAndSet(this, subscriber, null);
    }

    private void releaseInboundWithError(final Throwable error) {
        final Subscriber<?> inboundSubscriber = inboundSubscriberUpdater.getAndSet(this, null);
        if (null != inboundSubscriber && !inboundSubscriber.isUnsubscribed()) {
            try {
                inboundSubscriber.onError(error);
            } catch (final Exception e) {
                LOG.warn("exception when invoke {}.onError, detail: {}",
                    inboundSubscriber, ExceptionUtils.exception2detail(e));
            }
        }
    }

    private void fireClosed(final Throwable e) {
        this._selector.destroyAndSubmit(FIRE_CLOSED, this, e);
    }

    private static final ActionN FIRE_CLOSED = new ActionN() {
        @Override
        public void call(final Object... args) {
            ((DefaultRedisConnection)args[0]).doClosed((Throwable)args[1]);
        }
    };

    private void doClosed(final Throwable e) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("close active redis connection[channel: {}] by {}",
                    this._channel,
                    errorAsString(e));
        }

        removeInboundHandler();

        // notify response Subscriber with error
        releaseInboundWithError(e);

        unsubscribeOutbound();

        this._endSupport.fireAllActions(this);
    }

    private static String errorAsString(final Throwable e) {
        return e != null
            ?
                (e instanceof CloseException)
                ? "close()"
                : ExceptionUtils.exception2detail(e)
            : "no error"
            ;
    }

    private void setInboundHandler(final ChannelHandler handler) {
        inboundHandlerUpdater.set(this, handler);
    }

    private void removeInboundHandler() {
        final ChannelHandler handler = inboundHandlerUpdater.getAndSet(this, null);
        if (null != handler) {
            Nettys.actionToRemoveHandler(this._channel, handler).call();
        }
    }

    private void doSetOutbound(final Observable<? extends RedisMessage> request) {
        setOutboundSubscription(request.subscribe(buildOutboundObserver()));
    }

    private Observer<RedisMessage> buildOutboundObserver() {
        return new Observer<RedisMessage>() {
                @Override
                public void onCompleted() {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("outound invoke onCompleted for connection: {}", DefaultRedisConnection.this);
                    }
                }

                @Override
                public void onError(final Throwable e) {
                    if (!(e instanceof CloseException)) {
                        LOG.warn("outound invoke onError with ({}), try close connection: {}",
                                ExceptionUtils.exception2detail(e), DefaultRedisConnection.this);
                    }
                    fireClosed(e);
                }

                @Override
                public void onNext(final RedisMessage outmsg) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("outbound invoke onNext({}) for connection: {}", outmsg, DefaultRedisConnection.this);
                    }
                    _op.sendOutmsg(DefaultRedisConnection.this, outmsg);
                }};
    }

    private void setOutboundSubscription(final Subscription subscription) {
        outboundSubscriptionUpdater.set(this, subscription);
    }

    private void unsubscribeOutbound() {
        final Subscription subscription = outboundSubscriptionUpdater.getAndSet(this, null);
        if (null != subscription && !subscription.isUnsubscribed()) {
            subscription.unsubscribe();
        }
    }

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<DefaultRedisConnection, Subscriber> inboundSubscriberUpdater =
            AtomicReferenceFieldUpdater.newUpdater(DefaultRedisConnection.class, Subscriber.class, "_inboundSubscriber");

    private volatile Subscriber<?> _inboundSubscriber;

    private static final AtomicReferenceFieldUpdater<DefaultRedisConnection, ChannelHandler> inboundHandlerUpdater =
            AtomicReferenceFieldUpdater.newUpdater(DefaultRedisConnection.class, ChannelHandler.class, "_inboundHandler");

    @SuppressWarnings("unused")
    private volatile ChannelHandler _inboundHandler;

    private static final AtomicReferenceFieldUpdater<DefaultRedisConnection, Subscription> outboundSubscriptionUpdater =
            AtomicReferenceFieldUpdater.newUpdater(DefaultRedisConnection.class, Subscription.class, "_outboundSubscription");

    private volatile Subscription _outboundSubscription;

    private static final AtomicIntegerFieldUpdater<DefaultRedisConnection> transactingUpdater =
            AtomicIntegerFieldUpdater.newUpdater(DefaultRedisConnection.class, "_isTransacting");

    @SuppressWarnings("unused")
    private volatile int _isTransacting = 0;

    private final long _createTimeMillis = System.currentTimeMillis();

    private void subscribeResponse(
            final Observable<? extends RedisMessage> request,
            final Subscriber<? super RedisMessage> subscriber) {
        if (subscriber.isUnsubscribed()) {
            LOG.info("response subscriber ({}) has been unsubscribed, ignore", subscriber);
            return;
        }
        if (holdInboundSubscriber(subscriber)) {
            // _inboundSubscriber field set to subscriber
            final ChannelHandler handler = new SimpleChannelInboundHandler<RedisMessage>() {
                @Override
                protected void channelRead0(final ChannelHandlerContext ctx,
                        final RedisMessage inmsg) throws Exception {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("channel({})/handler({}): channelRead0 and call with msg({}).",
                            ctx.channel(), ctx.name(), inmsg);
                    }
                    _op.onInmsgRecvd(DefaultRedisConnection.this, subscriber, inmsg);
                }};
            Nettys.applyHandler(this._channel.pipeline(), RedisHandlers.ON_MESSAGE, handler);
            setInboundHandler(handler);

            doSetOutbound(request);

            subscriber.add(Subscriptions.create(()->_op.doOnUnsubscribeResponse(this, subscriber)));
        } else {
            // _respSubscriber field has already setted
            subscriber.onError(new RuntimeException("response subscriber already setted."));
        }
    }

    private void doSendOutmsg(final RedisMessage outmsg) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("sending redis request msg {}", outmsg);
        }
        // set in transacting flag
        setTransacting();
        this._channel.writeAndFlush(ReferenceCountUtil.retain(outmsg))
        .addListener(future -> {
                if (future.isSuccess()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("send redis request msg {} success.", outmsg);
                    }
                } else {
                    LOG.warn("exception when send redis req: {}, detail: {}",
                            outmsg, ExceptionUtils.exception2detail(future.cause()));
                    fireClosed(new TransportException("send reqmsg error", future.cause()));
                }
            });
    }

    private void processInmsg(
            final Subscriber<? super RedisMessage> subscriber,
            final RedisMessage inmsg) {
        if (unholdInboundSubscriber(subscriber)) {
            removeInboundHandler();
            clearTransacting();
            try {
                subscriber.onNext(inmsg);
            } finally {
                subscriber.onCompleted();
            }
        }
    }

    private void doOnUnsubscribeResponse(final Subscriber<? super RedisMessage> subscriber) {
        if (unholdInboundSubscriber(subscriber)) {
            removeInboundHandler();
            // unsubscribe before OnCompleted or OnError
            fireClosed(new RuntimeException("unsubscribe response"));
        }
    }

    private void clearTransacting() {
        transactingUpdater.compareAndSet(DefaultRedisConnection.this, 1, 0);
    }

    private void setTransacting() {
        // TODO, using this field as counter for req redis count & resp redis count
        transactingUpdater.compareAndSet(DefaultRedisConnection.this, 0, 1);
    }

    protected interface Op {
        public void subscribeResponse(
                final DefaultRedisConnection connection,
                final Observable<? extends RedisMessage> request,
                final Subscriber<? super RedisMessage> subscriber);

        public void doOnUnsubscribeResponse(
                final DefaultRedisConnection connection,
                final Subscriber<? super RedisMessage> subscriber);

        public void onInmsgRecvd(
                final DefaultRedisConnection connection,
                final Subscriber<? super RedisMessage> subscriber,
                final RedisMessage msg);

        public void sendOutmsg(
                final DefaultRedisConnection connection,
                final RedisMessage msg);
    }

    private static final Op OP_ACTIVE = new Op() {
        @Override
        public void subscribeResponse(
                final DefaultRedisConnection connection,
                final Observable<? extends RedisMessage> request,
                final Subscriber<? super RedisMessage> subscriber) {
            connection.subscribeResponse(request, subscriber);
        }

        @Override
        public void doOnUnsubscribeResponse(
                final DefaultRedisConnection connection,
                final Subscriber<? super RedisMessage> subscriber) {
            connection.doOnUnsubscribeResponse(subscriber);
        }

        @Override
        public void onInmsgRecvd(
                final DefaultRedisConnection connection,
                final Subscriber<? super RedisMessage> subscriber,
                final RedisMessage inmsg) {
            connection.processInmsg(subscriber, inmsg);
        }

        @Override
        public void sendOutmsg(
                final DefaultRedisConnection connection,
                final RedisMessage msg) {
            connection.doSendOutmsg(msg);
        }
    };

    private static final Op OP_UNACTIVE = new Op() {
        @Override
        public void subscribeResponse(
                final DefaultRedisConnection connection,
                final Observable<? extends RedisMessage> request,
                final Subscriber<? super RedisMessage> subscriber) {
            subscriber.onError(new RuntimeException("redis connection unactive."));
        }

        @Override
        public void doOnUnsubscribeResponse(
                final DefaultRedisConnection connection,
                final Subscriber<? super RedisMessage> subscriber) {
        }

        @Override
        public void onInmsgRecvd(
                final DefaultRedisConnection connection,
                final Subscriber<? super RedisMessage> subscriber,
                final RedisMessage msg) {
        }

        @Override
        public void sendOutmsg(
                final DefaultRedisConnection connection,
                final RedisMessage msg) {
        }
    };

    private static final AtomicInteger _IDSRC = new AtomicInteger(0);

    private final int _id = _IDSRC.getAndIncrement();

    @Override
    public int compareTo(final DefaultRedisConnection o) {
        return this._id - o._id;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this._id;
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final DefaultRedisConnection other = (DefaultRedisConnection) obj;
        if (this._id != other._id)
            return false;
        return true;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("DefaultRedisConnection [create at:")
                .append(new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new Date(this._createTimeMillis)))
                .append(", isActive=").append(isActive())
                .append(", isTransacting=").append(isTransacting())
                .append(", outboundSubscription=").append(_outboundSubscription)
                .append(", inboundSubscriber=").append(_inboundSubscriber)
                .append(", channel=").append(_channel)
                .append("]");
        return builder.toString();
    }
}
