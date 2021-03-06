package com.example.redislock;

import lombok.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.integration.redis.util.RedisLockRegistry;
import org.springframework.integration.support.locks.ExpirableLockRegistry;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

@SpringBootApplication
public class RedisLockApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisLockApplication.class, args);
    }

    @Bean
    public RedisLockRegistry defaultLockRegistry(RedisConnectionFactory connectionFactory) {
        return new RedisLockRegistry(connectionFactory, "gc:registry_key", Duration.ofMinutes(5).toMillis());
    }
}

@RestController
@RequiredArgsConstructor
class RedisLockController {

    private final ExpirableLockRegistry lockRegistry;
    private final MessageRepository repository;

    @SneakyThrows
    @GetMapping("/{key}/{message}/{sleep}")
    public Message message(@PathVariable String key, @PathVariable String message, @PathVariable long sleep) {
        Lock lock = lockRegistry.obtain(key);
        boolean lockAcquired = lock.tryLock(30, TimeUnit.SECONDS);
        if (!lockAcquired) {
            throw new RuntimeException("Cannot acquire lock for key: " + key);
        }
        try {
            Message m = repository.get(key);
            Thread.sleep(sleep);
            if (m == null) {
                repository.set(key, new Message(message, BigDecimal.valueOf(1), Instant.now()));
            } else {
                return m;
            }
        } finally {
            lock.unlock();
        }
        return repository.get(key);
    }

    @GetMapping("/{key}")
    public Message message(@PathVariable String key) {
        return repository.get(key);
    }

    /**
     * IMPORTANT: Starting with version 5.0, the RedisLockRegistry implements ExpirableLockRegistry,
     * which removes locks last acquired more than age ago and that are not currently locked.
     *
     * {@link ExpirableLockRegistry#expireUnusedOlderThan(long)} has to be called if the key is unique on each lock
     */
    @Scheduled(fixedDelay = 1000)
    protected void cleanObsoleteInMemoryLocks() {
        lockRegistry.expireUnusedOlderThan(Duration.ofSeconds(30).toMillis());
    }
}

@Repository
class MessageRepository {

    private final ValueOperations<String, Message> valueOps;

    MessageRepository(RedisTemplate redisTemplate) {
        this.valueOps = redisTemplate.opsForValue();
    }

    public void set(String key, Message m) {
        valueOps.set("tmp:MESSAGES" + key, m, Duration.ofSeconds(60L));
    }

    public Message get(String key) {
        return valueOps.get("tmp:MESSAGES" + key);
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Message implements Serializable {

    private String text;
    private BigDecimal bd;
    private Instant time;
}