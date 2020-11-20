/**
 * Copyright 2016 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;

import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandExecutor;
import org.redisson.core.RLock;
import org.redisson.pubsub.LockPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.PlatformDependent;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p/>
 * Implements a <b>non-fair</b> locking so doesn't guarantees an acquire order.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonLock extends RedissonExpirable implements RLock {

    private final Logger log = LoggerFactory.getLogger(RedissonLock.class);
    
    public static final long LOCK_EXPIRATION_INTERVAL_SECONDS = 30;
    private static final ConcurrentMap<String, Timeout> expirationRenewalMap = PlatformDependent.newConcurrentHashMap();
    protected long internalLockLeaseTime = TimeUnit.SECONDS.toMillis(LOCK_EXPIRATION_INTERVAL_SECONDS);

    final UUID id;

    protected static final LockPubSub PUBSUB = new LockPubSub();

    final CommandExecutor commandExecutor;

    protected RedissonLock(CommandExecutor commandExecutor, String name, UUID id) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.id = id;
    }

    protected String getEntryName() {
        return id + ":" + getName();
    }

    String getChannelName() {
        if (getName().contains("{")) {
            return "redisson_lock__channel:" + getName();
        }
        return "redisson_lock__channel__{" + getName() + "}";
    }

    String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lock(long leaseTime, TimeUnit unit) {
        try {
            lockInterruptibly(leaseTime, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    @Override
    public void lockInterruptibly() throws InterruptedException {
        lockInterruptibly(-1, null);
    }

    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        Long ttl = tryAcquire(leaseTime, unit);
        // lock acquired
        if (ttl == null) {
            return;
        }

        long threadId = Thread.currentThread().getId();
        Future<RedissonLockEntry> future = subscribe(threadId);
        get(future);

        try {
            while (true) {
                ttl = tryAcquire(leaseTime, unit);
                // lock acquired
                if (ttl == null) {
                    break;
                }

                // waiting for message
                if (ttl >= 0) {
                    getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    getEntry(threadId).getLatch().acquire();
                }
            }
        } finally {
            unsubscribe(future, threadId);
        }
//        get(lockAsync(leaseTime, unit));
    }
    
    private Long tryAcquire(long leaseTime, TimeUnit unit) {
        return get(tryAcquireAsync(leaseTime, unit, Thread.currentThread().getId()));
    }
    
    private Future<Boolean> tryAcquireOnceAsync(long leaseTime, TimeUnit unit, long threadId) {
        if (leaseTime != -1) {
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        }
        Future<Boolean> ttlRemainingFuture = tryLockInnerAsync(LOCK_EXPIRATION_INTERVAL_SECONDS, TimeUnit.SECONDS, threadId, RedisCommands.EVAL_NULL_BOOLEAN);
        ttlRemainingFuture.addListener(new FutureListener<Boolean>() {
            @Override
            public void operationComplete(Future<Boolean> future) throws Exception {
                if (!future.isSuccess()) {
                    return;
                }

                Boolean ttlRemaining = future.getNow();
                // lock acquired
                if (ttlRemaining) {
                    scheduleExpirationRenewal();
                }
            }
        });
        return ttlRemainingFuture;
    }

    private <T> Future<Long> tryAcquireAsync(long leaseTime, TimeUnit unit, long threadId) {
        if (leaseTime != -1) {
            return tryLockInnerAsync(leaseTime, unit, threadId, RedisCommands.EVAL_LONG);
        }
        Future<Long> ttlRemainingFuture = tryLockInnerAsync(LOCK_EXPIRATION_INTERVAL_SECONDS, TimeUnit.SECONDS, threadId, RedisCommands.EVAL_LONG);
        ttlRemainingFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    return;
                }

                Long ttlRemaining = future.getNow();
                // lock acquired
                if (ttlRemaining == null) {
                    scheduleExpirationRenewal();
                }
            }
        });
        return ttlRemainingFuture;
    }

    @Override
    public boolean tryLock() {
        return get(tryLockAsync());
    }

    private void scheduleExpirationRenewal() {
        if (expirationRenewalMap.containsKey(getEntryName())) {
            return;
        }

        Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                Future<Boolean> future = expireAsync(internalLockLeaseTime, TimeUnit.MILLISECONDS);
                future.addListener(new FutureListener<Boolean>() {
                    @Override
                    public void operationComplete(Future<Boolean> future) throws Exception {
                        expirationRenewalMap.remove(getEntryName());
                        if (!future.isSuccess()) {
                            log.error("Can't update lock " + getName() + " expiration", future.cause());
                            return;
                        }
                        
                        if (future.getNow()) {
                            // reschedule itself
                            scheduleExpirationRenewal();
                        }
                    }
                });
            }
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);

        if (expirationRenewalMap.putIfAbsent(getEntryName(), task) != null) {
            task.cancel();
        }
    }

    void cancelExpirationRenewal() {
        Timeout task = expirationRenewalMap.remove(getEntryName());
        if (task != null) {
            task.cancel();
        }
    }

    <T> Future<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);

        /**
         * 通过 EVAL 命令执行 Lua 脚本获取锁，保证了原子性
         */
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                // 1.如果缓存中的key不存在，则执行 hset 命令(hset key UUID+threadId 1),然后通过 pexpire 命令设置锁的过期时间(即锁的租约时间)
                // 返回空值 nil ，表示获取锁成功
                  "if (redis.call('exists', KEYS[1]) == 0) then " +
                      "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                      "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                      "return nil; " +
                  "end; " +
                 // 如果key已经存在，并且value也匹配，表示是当前线程持有的锁，则执行 hincrby 命令，重入次数加1，并且设置失效时间
                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                      "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                      "return nil; " +
                  "end; " +
                 //如果key已经存在，但是value不匹配，说明锁已经被其他线程持有，通过 pttl 命令获取锁的剩余存活时间并返回，至此获取锁失败
                  "return redis.call('pttl', KEYS[1]);",
                 //这三个参数分别对应KEYS[1]，ARGV[1]和ARGV[2]
                /**
                 * 参数说明：
                 *
                 * KEYS[1]就是Collections.singletonList(getName())，表示分布式锁的key；
                 *
                 * ARGV[1]就是internalLockLeaseTime，即锁的租约时间（持有锁的有效时间），默认30s；
                 *
                 * ARGV[2]就是getLockName(threadId)，是获取锁时set的唯一值 value，即UUID+threadId。
                 *
                 */
                 Collections.<Object>singletonList(getName()), internalLockLeaseTime, getLockName(threadId));
    }

    @Override
    public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
        //取得最大等待时间
        long time = unit.toMillis(waitTime);
        //尝试申请锁，返回还剩余的锁过期时间
        Long ttl = tryAcquire(leaseTime, unit);
        // lock acquired
        //如果为空，表示申请锁成功
        if (ttl == null) {
            return true;
        }

        //取得当前线程id（判断是否可重入锁的关键）
        final long threadId = Thread.currentThread().getId();
        /**
         * 订阅锁释放事件，并通过await方法阻塞等待锁释放，有效的解决了无效的锁申请浪费资源的问题：
         * 基于信息量，当锁被其它资源占用时，当前线程通过 Redis 的 channel 订阅锁的释放事件，一旦锁释放会发消息通知待等待的线程进行竞争
         * 当 this.await返回false，说明等待时间已经超出获取锁最大等待时间，取消订阅并返回获取锁失败
         * 当 this.await返回true，进入循环尝试获取锁
         */
        Future<RedissonLockEntry> future = subscribe(threadId);
        //await 方法内部是用CountDownLatch来实现阻塞，获取subscribe异步执行的结果（应用了Netty 的 Future）
        if (!await(future, time, TimeUnit.MILLISECONDS)) {
            if (!future.cancel(false)) {
                future.addListener(new FutureListener<RedissonLockEntry>() {
                    @Override
                    public void operationComplete(Future<RedissonLockEntry> future) throws Exception {
                        if (future.isSuccess()) {
                            unsubscribe(future, threadId);
                        }
                    }
                });
            }
            return false;
        }

        try {
            while (true) {
                // 再次尝试申请锁
                ttl = tryAcquire(leaseTime, unit);
                // lock acquired
                // 成功获取锁则直接返回true结束循环
                if (ttl == null) {
                    return true;
                }

                //超过最大等待时间则返回false结束循环，获取锁失败
                if (time <= 0) {
                    return false;
                }

                // waiting for message
                //阻塞等待锁（通过信号量(共享锁)阻塞,等待解锁消息）
                long current = System.currentTimeMillis();
                if (ttl >= 0 && ttl < time) {
                    //如果剩余时间(ttl)小于wait time ,就在 ttl 时间内，从Entry的信号量获取一个许可(除非被中断或者一直没有可用的许可)。
                    getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    //则就在wait time 时间范围内等待可以通过信号量
                    getEntry(threadId).getLatch().tryAcquire(time, TimeUnit.MILLISECONDS);
                }

                long elapsed = System.currentTimeMillis() - current;
                time -= elapsed;
            }
        } finally {
            //无论是否获得锁,都要取消订阅解锁消息
            unsubscribe(future, threadId);
        }
//        return get(tryLockAsync(waitTime, leaseTime, unit));
    }

    protected RedissonLockEntry getEntry(long threadId) {
        return PUBSUB.getEntry(getEntryName());
    }

    protected Future<RedissonLockEntry> subscribe(long threadId) {
        return PUBSUB.subscribe(getEntryName(), getChannelName(), commandExecutor.getConnectionManager());
    }

    protected void unsubscribe(Future<RedissonLockEntry> future, long threadId) {
        PUBSUB.unsubscribe(future.getNow(), getEntryName(), getChannelName(), commandExecutor.getConnectionManager());
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        return tryLock(waitTime, -1, unit);
    }

    @Override
    public void unlock() {
        Boolean opStatus = commandExecutor.evalWrite(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "if (redis.call('exists', KEYS[1]) == 0) then " +
                            "redis.call('publish', KEYS[2], ARGV[1]); " +
                            "return 1; " +
                        "end;" +
                        "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                            "return nil;" +
                        "end; " +
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "return 0; " +
                        "else " +
                            "redis.call('del', KEYS[1]); " +
                            "redis.call('publish', KEYS[2], ARGV[1]); " +
                            "return 1; "+
                        "end; " +
                        "return nil;",
                        Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.unlockMessage, internalLockLeaseTime, getLockName(Thread.currentThread().getId()));
        if (opStatus == null) {
            throw new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                    + id + " thread-id: " + Thread.currentThread().getId());
        }
        if (opStatus) {
            cancelExpirationRenewal();
        }

//        Future<Void> future = unlockAsync();
//        future.awaitUninterruptibly();
//        if (future.isSuccess()) {
//            return;
//        }
//        if (future.cause() instanceof IllegalMonitorStateException) {
//            throw (IllegalMonitorStateException)future.cause();
//        }
//        throw commandExecutor.convertException(future);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public void forceUnlock() {
        get(forceUnlockAsync());
    }

    @Override
    public Future<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal();
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('del', KEYS[1]) == 1) then "
                + "redis.call('publish', KEYS[2], ARGV[1]); "
                + "return 1 "
                + "else "
                + "return 0 "
                + "end",
                Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.unlockMessage);
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return commandExecutor.write(getName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getName(), getLockName(Thread.currentThread().getId()));
    }

    @Override
    public int getHoldCount() {
        Long res = commandExecutor.write(getName(), LongCodec.INSTANCE, RedisCommands.HGET, getName(), getLockName(Thread.currentThread().getId()));
        if (res == null) {
            return 0;
        }
        return res.intValue();
    }

    @Override
    public Future<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    public Future<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    public Future<Void> unlockAsync(final long threadId) {
        final Promise<Void> result = newPromise();
        Future<Boolean> future = commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "if (redis.call('exists', KEYS[1]) == 0) then " +
                            "redis.call('publish', KEYS[2], ARGV[1]); " +
                            "return 1; " +
                        "end;" +
                        "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                            "return nil;" +
                        "end; " +
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "return 0; " +
                        "else " +
                            "redis.call('del', KEYS[1]); " +
                            "redis.call('publish', KEYS[2], ARGV[1]); " +
                            "return 1; "+
                        "end; " +
                        "return nil;",
                        Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.unlockMessage, internalLockLeaseTime, getLockName(threadId));

        future.addListener(new FutureListener<Boolean>() {
            @Override
            public void operationComplete(Future<Boolean> future) throws Exception {
                if (!future.isSuccess()) {
                    result.setFailure(future.cause());
                    return;
                }

                Boolean opStatus = future.getNow();
                if (opStatus == null) {
                    IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                            + id + " thread-id: " + threadId);
                    result.setFailure(cause);
                    return;
                }
                if (opStatus) {
                    cancelExpirationRenewal();
                }
                result.setSuccess(null);
            }
        });

        return result;
    }

    public Future<Void> lockAsync() {
        return lockAsync(-1, null);
    }

    public Future<Void> lockAsync(final long leaseTime, final TimeUnit unit) {
        final long currentThreadId = Thread.currentThread().getId();
        return lockAsync(leaseTime, unit, currentThreadId);
    }

    public Future<Void> lockAsync(final long leaseTime, final TimeUnit unit, final long currentThreadId) {
        final Promise<Void> result = newPromise();
        Future<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    result.setFailure(future.cause());
                    return;
                }

                Long ttl = future.getNow();

                // lock acquired
                if (ttl == null) {
                    result.setSuccess(null);
                    return;
                }

                final Future<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
                subscribeFuture.addListener(new FutureListener<RedissonLockEntry>() {
                    @Override
                    public void operationComplete(Future<RedissonLockEntry> future) throws Exception {
                        if (!future.isSuccess()) {
                            result.setFailure(future.cause());
                            return;
                        }

                        lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                    }

                });
            }
        });

        return result;
    }

    private void lockAsync(final long leaseTime, final TimeUnit unit,
            final Future<RedissonLockEntry> subscribeFuture, final Promise<Void> result, final long currentThreadId) {
        Future<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.setFailure(future.cause());
                    return;
                }

                Long ttl = future.getNow();
                // lock acquired
                if (ttl == null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.setSuccess(null);
                    return;
                }

                // waiting for message
                final RedissonLockEntry entry = getEntry(currentThreadId);
                synchronized (entry) {
                    if (entry.getLatch().tryAcquire()) {
                        lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                    } else {
                        final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<ScheduledFuture<?>>();
                        final Runnable listener = new Runnable() {
                            @Override
                            public void run() {
                                if (futureRef.get() != null) {
                                    futureRef.get().cancel(false);
                                }
                                lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                            }
                        };

                        entry.addListener(listener);

                        if (ttl >= 0) {
                            ScheduledFuture<?> scheduledFuture = commandExecutor.getConnectionManager().getGroup().schedule(new Runnable() {
                                @Override
                                public void run() {
                                    synchronized (entry) {
                                        if (entry.removeListener(listener)) {
                                            lockAsync(leaseTime, unit, subscribeFuture, result, currentThreadId);
                                        }
                                    }
                                }
                            }, ttl, TimeUnit.MILLISECONDS);
                            futureRef.set(scheduledFuture);
                        }
                    }
                }
            }
        });
    }

    public Future<Boolean> tryLockAsync() {
        return tryLockAsync(Thread.currentThread().getId());
    }

    public Future<Boolean> tryLockAsync(long threadId) {
        return tryAcquireOnceAsync(-1, null, threadId);
    }

    public Future<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        return tryLockAsync(waitTime, -1, unit);
    }

    public Future<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
        long currentThreadId = Thread.currentThread().getId();
        return tryLockAsync(waitTime, leaseTime, unit, currentThreadId);
    }

    public Future<Boolean> tryLockAsync(final long waitTime, final long leaseTime, final TimeUnit unit,
            final long currentThreadId) {
        final Promise<Boolean> result = newPromise();

        final AtomicLong time = new AtomicLong(unit.toMillis(waitTime));
        Future<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    result.setFailure(future.cause());
                    return;
                }

                Long ttl = future.getNow();

                // lock acquired
                if (ttl == null) {
                    result.setSuccess(true);
                    return;
                }

                final long current = System.currentTimeMillis();
                final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<ScheduledFuture<?>>();
                final Future<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
                subscribeFuture.addListener(new FutureListener<RedissonLockEntry>() {
                    @Override
                    public void operationComplete(Future<RedissonLockEntry> future) throws Exception {
                        if (!future.isSuccess()) {
                            result.tryFailure(future.cause());
                            return;
                        }

                        if (futureRef.get() != null) {
                            futureRef.get().cancel(false);
                        }

                        long elapsed = System.currentTimeMillis() - current;
                        time.addAndGet(-elapsed);
                        
                        if (time.get() < 0) {
                            unsubscribe(subscribeFuture, currentThreadId);
                            result.trySuccess(false);
                            return;
                        }

                        tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                    }
                });
                if (!subscribeFuture.isDone()) {
                    ScheduledFuture<?> scheduledFuture = commandExecutor.getConnectionManager().getGroup().schedule(new Runnable() {
                        @Override
                        public void run() {
                            if (!subscribeFuture.isDone()) {
                                subscribeFuture.cancel(false);
                                result.trySuccess(false);
                            }
                        }
                    }, time.get(), TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });


        return result;
    }

    private void tryLockAsync(final AtomicLong time, final long leaseTime, final TimeUnit unit,
            final Future<RedissonLockEntry> subscribeFuture, final Promise<Boolean> result, final long currentThreadId) {
        Future<Long> ttlFuture = tryAcquireAsync(leaseTime, unit, currentThreadId);
        ttlFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.tryFailure(future.cause());
                    return;
                }

                Long ttl = future.getNow();
                // lock acquired
                if (ttl == null) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.trySuccess(true);
                    return;
                }
                
                if (time.get() < 0) {
                    unsubscribe(subscribeFuture, currentThreadId);
                    result.trySuccess(false);
                    return;
                }

                // waiting for message
                final long current = System.currentTimeMillis();
                final RedissonLockEntry entry = getEntry(currentThreadId);
                synchronized (entry) {
                    if (entry.getLatch().tryAcquire()) {
                        tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                    } else {
                        final AtomicBoolean executed = new AtomicBoolean();
                        final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<ScheduledFuture<?>>();
                        final Runnable listener = new Runnable() {
                            @Override
                            public void run() {
                                executed.set(true);
                                if (futureRef.get() != null) {
                                    futureRef.get().cancel(false);
                                }
                                long elapsed = System.currentTimeMillis() - current;
                                time.addAndGet(-elapsed);

                                tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                            }
                        };
                        entry.addListener(listener);

                        long t = time.get();
                        if (ttl >= 0 && ttl < time.get()) {
                            t = ttl;
                        }
                        if (!executed.get()) {
                            ScheduledFuture<?> scheduledFuture = commandExecutor.getConnectionManager().getGroup().schedule(new Runnable() {
                                @Override
                                public void run() {
                                    synchronized (entry) {
                                        if (entry.removeListener(listener)) {
                                            long elapsed = System.currentTimeMillis() - current;
                                            time.addAndGet(-elapsed);

                                            tryLockAsync(time, leaseTime, unit, subscribeFuture, result, currentThreadId);
                                        }
                                    }
                                }
                            }, t, TimeUnit.MILLISECONDS);
                            futureRef.set(scheduledFuture);
                        }
                    }
                }
            }
        });
    }


}
