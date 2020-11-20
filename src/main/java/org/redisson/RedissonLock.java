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

    /**
     * 该方法在 RLock 中声明，支持对获取锁的线程进行中断操作
     */
    @Override
    public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
        // 1.尝试获取锁
        /**
         * 首先尝试获取锁，具体代码下面再看，返回结果是已存在的锁的剩余存活时间，为 null 则说明没有已存在的锁并成功获得锁
         */
        Long ttl = tryAcquire(leaseTime, unit);
        // lock acquired
        // 2.获得锁成功
        if (ttl == null) {
            return;// 如果获得锁则结束流程，回去执行业务逻辑
        }

        // 3.等待锁释放，并订阅锁
        /**
         * 如果没有获得锁，则需等待锁被释放，并通过 Redis 的 channel 订阅锁释放的消息，这里的具体实现本文也不深入，
         * 只是简单提一下 Redisson 在执行 Redis 命令时提供了同步和异步的两种实现，但实际上同步的实现都是基于异步的，
         * 具体做法是使用 Netty 中的异步工具 Future 和 FutureListener 结合 JDK 中的 CountDownLatch 一起实现。
         */
        long threadId = Thread.currentThread().getId();
        Future<RedissonLockEntry> future = subscribe(threadId);
        get(future);

        /**
         * 订阅锁的释放消息成功后，进入一个不断重试获取锁的循环，循环中每次都先试着获取锁，并得到已存在的锁的剩余存活时间
         */
        try {
            while (true) {
                // 4.重试获取锁
                ttl = tryAcquire(leaseTime, unit);
                // lock acquired
                // 5.成功获得锁
                if (ttl == null) {
                    break;// 如果在重试中拿到了锁，则结束循环，跳过第 6 步
                }

                // 6.等待锁释放
                /**
                 * 如果锁当前是被占用的，那么等待释放锁的消息，具体实现使用了 JDK 并发的信号量工具 Semaphore 来阻塞线程，
                 * 当锁释放并发布释放锁的消息后，信号量的 release() 方法会被调用，此时被信号量阻塞的等待队列中的一个线程就
                 * 可以继续尝试获取锁了
                 */
                // waiting for message
                if (ttl >= 0) {
                    getEntry(threadId).getLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    getEntry(threadId).getLatch().acquire();
                }
            }
        } finally {
            // 7.取消订阅
            /**
             * 在成功获得锁后，就没必要继续订阅锁的释放消息了，因此要取消对 Redis 上相应 channel 的订阅
             */
            unsubscribe(future, threadId);
        }
//        get(lockAsync(leaseTime, unit));
    }
    
    private Long tryAcquire(long leaseTime, TimeUnit unit) {
        // 将异步执行的结果以同步的形式返回（Redisson 实现的执行 Redis 命令都是异步的，但是它在异步的基础上提供了以同步的方式获得执行结果的封装）
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
        // 2.用默认的锁超时时间去获取锁
        /**
         * 分布式锁要确保未来的一段时间内锁一定能够被释放，因此要对锁设置超时释放的时间，在我们没有指定该时间的情况下，Redisson 默认指定为30秒
         */
        Future<Long> ttlRemainingFuture = tryLockInnerAsync(LOCK_EXPIRATION_INTERVAL_SECONDS, TimeUnit.SECONDS, threadId, RedisCommands.EVAL_LONG);
        ttlRemainingFuture.addListener(new FutureListener<Long>() {
            @Override
            public void operationComplete(Future<Long> future) throws Exception {
                if (!future.isSuccess()) {
                    return;
                }

                Long ttlRemaining = future.getNow();
                // lock acquired
                // 成功获得锁
                if (ttlRemaining == null) {
                    // 3.锁过期时间刷新任务调度
                    /**
                     * 在成功获取到锁的情况下，为了避免业务中对共享资源的操作还未完成，锁就被释放掉了，需要定期（锁失效时间的三分之一）刷新锁失效的时间，这里 Redisson 使用了 Netty 的 TimerTask、Timeout 工具来实现该任务调度
                     */
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

    /**
     * 这三个参数分别对应KEYS[1]，ARGV[1]和ARGV[2]，说明如下：
     *
     * KEYS[1]就是Collections.singletonList(getName())，表示分布式锁的key；
     *
     * ARGV[1]就是internalLockLeaseTime，即锁的租约时间（持有锁的有效时间），默认30s；
     *
     * ARGV[2]就是getLockName(threadId)，是获取锁时set的唯一值 value，即UUID+threadId。
     *
     */
    <T> Future<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);

        /**
         * 获取锁真正执行的命令，Redisson 使用 EVAL 命令执行上面的 Lua 脚本来完成获取锁的操作
         */
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                /**
                 * 如果通过 exists 命令发现当前 key 不存在，即锁没被占用，则执行 hset 写入 Hash 类型数据 key:全局锁名称（例如共享资源ID）, field:锁实例名称（Redisson客户端ID:线程ID）, value:1，并执行 pexpire 对该 key 设置失效时间，返回空值 nil，至此获取锁成功。
                 */
                  "if (redis.call('exists', KEYS[1]) == 0) then " +
                      "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                      "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                      "return nil; " +
                  "end; " +
                 // 如果通过 hexists 命令发现 Redis 中已经存在当前 key 和 field 的 Hash 数据，说明当前线程之前已经获取到锁，因为这里的锁是可重入的，则执行 hincrby 对当前 key field 的值加一，并重新设置失效时间，返回空值，至此重入获取锁成功。
                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                      "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                      "return nil; " +
                  "end; " +
                 //如果key已经存在，但是value不匹配，说明锁已经被其他线程持有，通过 pttl 命令获取锁的剩余存活时间并返回，至此获取锁失败
                  "return redis.call('pttl', KEYS[1]);",
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
        // 1.通过 EVAL 和 Lua 脚本执行 Redis 命令释放锁
        /**
         * 使用 EVAL 命令执行 Lua 脚本来释放锁
         */
        Boolean opStatus = commandExecutor.evalWrite(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        // key 不存在，说明锁已释放，直接执行 publish 命令发布释放锁消息并返回 1
                        "if (redis.call('exists', KEYS[1]) == 0) then " +
                            "redis.call('publish', KEYS[2], ARGV[1]); " +
                            "return 1; " +
                        "end;" +
                        // key 存在，但是 field 在 Hash 中不存在，说明自己不是锁持有者，无权释放锁，返回 nil
                        "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                            "return nil;" +
                        "end; " +
                        // 因为锁可重入，所以释放锁时不能把所有已获取的锁全都释放掉，一次只能释放一把锁，因此执行 hincrby 对锁的值减一
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "return 0; " +
                        // 释放一把锁后，如果还有剩余的锁，则刷新锁的失效时间并返回 0；如果刚才释放的已经是最后一把锁，则执行 del 命令删除锁的 key，并发布锁释放消息，返回 1
                        "else " +
                            "redis.call('del', KEYS[1]); " +
                            "redis.call('publish', KEYS[2], ARGV[1]); " +
                            "return 1; "+
                        "end; " +
                        "return nil;",
                        Arrays.<Object>asList(getName(), getChannelName()), LockPubSub.unlockMessage, internalLockLeaseTime, getLockName(Thread.currentThread().getId()));
        // 2.非锁的持有者释放锁时抛出异常
        if (opStatus == null) {
            throw new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                    + id + " thread-id: " + Thread.currentThread().getId());
        }
        // 3.释放锁后取消刷新锁失效时间的调度任务
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
