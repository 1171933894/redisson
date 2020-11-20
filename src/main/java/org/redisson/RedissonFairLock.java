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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandExecutor;
import org.redisson.core.RLock;
import org.redisson.pubsub.LockPubSub;

import io.netty.util.concurrent.Future;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p/>
 * Implements a <b>fair</b> locking so it guarantees an acquire order by threads.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonFairLock extends RedissonLock implements RLock {

    private final CommandExecutor commandExecutor;

    protected RedissonFairLock(CommandExecutor commandExecutor, String name, UUID id) {
        super(commandExecutor, name, id);
        this.commandExecutor = commandExecutor;
    }
    
    String getThreadsQueueName() {
        return "redisson_lock_queue:{" + getName() + "}";
    }
    
    String getThreadElementName(long threadId) {
        return "redisson_lock_thread:{" + getName() + "}:" + getLockName(threadId);
    }
    
    @Override
    protected RedissonLockEntry getEntry(long threadId) {
        return PUBSUB.getEntry(getEntryName() + ":" + threadId);
    }

    @Override
    protected Future<RedissonLockEntry> subscribe(long threadId) {
        return PUBSUB.subscribe(getEntryName() + ":" + threadId, 
                getChannelName() + ":" + getLockName(threadId), commandExecutor.getConnectionManager());
    }

    @Override
    protected void unsubscribe(Future<RedissonLockEntry> future, long threadId) {
        PUBSUB.unsubscribe(future.getNow(), getEntryName() + ":" + threadId, 
                getChannelName() + ":" + getLockName(threadId), commandExecutor.getConnectionManager());
    }

    @Override
    <T> Future<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);
        long threadWaitTime = 5000;

        if (command == RedisCommands.EVAL_NULL_BOOLEAN) {
            return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    "while true do "
                    + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                    + "if firstThreadId2 == false then "
                        + "break;"
                    + "end; "
                    + "if redis.call('exists', 'redisson_lock_thread:{' .. KEYS[1] .. '}:' .. firstThreadId2) == 0 then "
                        + "redis.call('lpop', KEYS[2]); "
                    + "else "
                        + "break;"
                    + "end; "
                  + "end;"
                    + 
                    
                    "if (redis.call('exists', KEYS[1]) == 0) and ((redis.call('exists', KEYS[2]) == 0) "
                            + "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                            "redis.call('lpop', KEYS[2]); " +
                            "redis.call('del', KEYS[3]); " +
                            "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                            "return nil; " +
                        "end; " +
                        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                            "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                            "return nil; " +
                        "end; " +
                        "return 1;", 
                    Arrays.<Object>asList(getName(), getThreadsQueueName(), getThreadElementName(threadId)), internalLockLeaseTime, getLockName(threadId));
        }

        // 公平锁源码入口
        if (command == RedisCommands.EVAL_LONG) {
            return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    // 1. 第一次，如果某个锁，没有人加锁，此时第一个客户端进来，先进入一个死循环
                    // 2. 第二个客户端进来尝试加锁，进入死循环中
                    // 3. 第三个客户端过来加锁，进入while true 的死循环当中
                    "while true do "
                    // 1. 第一次进来，这个lindex redisson_lock_queue:{anyLock} 0 的命令就是说，从 redisson_lock_queue:{lock}这个队列中弹出第一个元素，刚开始，肯定是空的，什么都没有，直接就会直接
                    // break掉，跳出死循环
                    // 2. 第二个客户端首先执行 lindex redisson_lock_queue:{anyLock} 0,取出队列的第一个元素，此时该队列还是空的，然后break掉，跳出死循环
                    // 3. lindex redisson_lock_queue:{anyLock} 0,取出队列的第一个元素，此时，这个队列中由于第二个客户端已经将数据插入到队列中，已经在排队了，所以值是有的firstThreadId2=10:00:25
                    + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                    + "if firstThreadId2 == false then "
                        + "break;"
                    + "end; "
                    + "if redis.call('exists', 'redisson_lock_thread:{' .. KEYS[1] .. '}:' .. firstThreadId2) == 0 then "
                        + "redis.call('lpop', KEYS[2]); "
                    + "else "
                        + "break;"
                    + "end; "
                  + "end;"
                    // 1. exists anyLock,锁不存在，也就是没人加锁，刚开始因为第一个客户端进来，之前肯定是没有人加锁的，这个条件是成立的
                    // 1. exists redisson_lock_queue:{anyLock}这个队列不存在，或者lindex redisson_lock_queue:{anyLock} 0 这个队列中的第一个元素是UUID:threadId ，
                    // 这个队列存在，但是排在这个队列的第一个元素是当前线程，那么此时这个条件就会成立。
                    // 第一个客户端进来的时候，其实anyLock和队列redisson_lock_queue:{anyLock}都是不存在的，所以条件是成立的

                    // 2. exists anyLock 是否存在，这个时候肯定已经存在了，所以这里的if 判断条件不成立
                    // 3. 第三个客户端进行条件判断，和第二个客户端是一样的，条件不成立
                    +
                        "if (redis.call('exists', KEYS[1]) == 0) and ((redis.call('exists', KEYS[2]) == 0) "
                            + "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                            "redis.call('lpop', KEYS[2]); " +
                            "redis.call('del', KEYS[3]); " +
                            "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                            "return nil; " +
                        "end; " +
                        "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                            "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                            "return nil; " +
                        "end; " +
                        "local firstThreadId = redis.call('lindex', KEYS[2], 0)" +
                        "local ttl = redis.call('pttl', KEYS[1]); " + 
                        "if firstThreadId ~= false and firstThreadId ~= ARGV[2] then " + 
                            "ttl = redis.call('pttl', 'redisson_lock_thread:{' .. KEYS[1] .. '}:' .. firstThreadId);" + 
                        "end; " + 
                        "if redis.call('exists', KEYS[3]) == 0 then " +
                            "redis.call('rpush', KEYS[2], ARGV[2]);" +
                            "redis.call('set', KEYS[3], 1);" +
                        "end; " +
                        "redis.call('pexpire', KEYS[3], ttl + tonumber(ARGV[3]));" +
                        "return ttl;", 
                        Arrays.<Object>asList(getName(), getThreadsQueueName(), getThreadElementName(threadId)), 
                                    internalLockLeaseTime, getLockName(threadId), threadWaitTime);
        }
        
        throw new IllegalArgumentException();
    }
    
    @Override
    public void unlock() {
        Boolean opStatus = commandExecutor.evalWrite(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "if redis.call('exists', 'redisson_lock_thread:{' .. KEYS[1] .. '}:' .. firstThreadId2) == 0 then "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                + 
                
                "if (redis.call('exists', KEYS[1]) == 0) then " + 
                    "local nextThreadId = redis.call('lindex', KEYS[3], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[2] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " +
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
                    "local nextThreadId = redis.call('lindex', KEYS[3], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[2] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " +
                    "return 1; "+
                "end; " +
                "return nil;",
                Arrays.<Object>asList(getName(), getChannelName(), getThreadsQueueName()), LockPubSub.unlockMessage, internalLockLeaseTime, getLockName(Thread.currentThread().getId()));
        
        if (opStatus == null) {
            throw new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                    + id + " thread-id: " + Thread.currentThread().getId());
        }
        if (opStatus) {
            cancelExpirationRenewal();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal();
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "if redis.call('exists', 'redisson_lock_thread:{' .. KEYS[1] .. '}:' .. firstThreadId2) == 0 then "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                + 
                
                "if (redis.call('del', KEYS[1]) == 1) then " + 
                    "local nextThreadId = redis.call('lindex', KEYS[3], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[2] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " + 
                    "return 1 " + 
                "end " + 
                "return 0;",
                Arrays.<Object>asList(getName(), getChannelName(), getThreadsQueueName()), LockPubSub.unlockMessage);
    }

}
