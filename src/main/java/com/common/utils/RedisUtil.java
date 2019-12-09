package com.common.utils;

import com.common.LocalConfig;
import com.common.constants.BusinessConstants.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@Order(0)
@DependsOn("localConfig")
public class RedisUtil {

	@Value("${spring.redis.host}")
	private String host;

	@Value("${spring.redis.port}")
	private Integer port;

	@Value("${spring.redis.password}")
	private String password;

	private static JedisPoolConfig jedisPoolConfig;

	/**
	 * set the value for key in db
	 * @param db
	 * @param key
	 * @param value
	 * @return
	 */
	public static String set(Integer db, String key, String value) {
		String result = "";
		JedisPool pool = RedisCache.getPool(db);
		if (null == pool || StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
			return result;
		}

		try (Jedis jedis = pool.getResource()) {
			result = jedis.set(key, value);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error happened when we set from db:[{}] by key:[{}], value:[{}}", db, key, value);
		}

		return result;
	}

	public static Boolean exists(Integer db, String key) {
		Boolean exists = false;
		JedisPool pool = RedisCache.getPool(db);
		if (null == pool || StringUtils.isBlank(key)) {
			return exists;
		}

		try (Jedis jedis = pool.getResource()) {
			exists = jedis.exists(key);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error happened when we check the key exists of db:[{}], key:[{}]"
			, db, key);
		}
		return exists;
	}

	public static Long del(Integer db, String key) {
		JedisPool pool = RedisCache.getPool(db);
		if (null == pool || StringUtils.isBlank(key)) {
			return 0L;
		}
		try (Jedis jedis = pool.getResource();) {
			return jedis.del(key);
		} catch (Exception e) {
			// TODO: handle exception
			log.error(e.getMessage());
			return null;
		}
	}

	public static Long hdel(Integer db,String key, String field) {
		JedisPool pool = RedisCache.getPool(db);
		if (null == pool || StringUtils.isBlank(key) || StringUtils.isBlank(field)) {
			return 0L;
		}
		try (Jedis jedis = pool.getResource();) {
			return jedis.hdel(key, field);
		} catch (Exception e) {
			// TODO: handle exception
			log.error(e.getMessage());
			return null;
		}
	}

	public static String hget(Integer db,String key, String field) {
		JedisPool pool = RedisCache.getPool(db);
		if (null == pool || StringUtils.isBlank(key) || StringUtils.isBlank(field)) {
			return "";
		}
		try (Jedis jedis = pool.getResource();) {
			return jedis.hget(key, field);
		} catch (Exception e) {
			// TODO: handle exception
			log.error(e.getMessage());
			return null;
		}
	}

	public static Map<String, String> hgetAll(Integer db,String key) {
		JedisPool pool = RedisCache.getPool(db);
		if (null == pool || StringUtils.isBlank(key)) {
			return null;
		}
		try (Jedis jedis = pool.getResource();) {
			return jedis.hgetAll(key);
		} catch (Exception e) {
			// TODO: handle exception
			log.error(e.getMessage());
			return null;
		}
	}

	public static void hset(Integer db,String key, String field, String value) {
		JedisPool pool = RedisCache.getPool(db);
		if (null == pool || StringUtils.isBlank(key)|| StringUtils.isBlank(field)|| StringUtils.isBlank(value)) {
			return;
		}
		try (Jedis jedis = pool.getResource();) {
			jedis.hset(key, field, value);
		} catch (Exception e) {
			// TODO: handle exception
			log.error(e.getMessage());
		}
	}

	public static String hmset(Integer db,String key, Map<String, String> info) {
		String result = "";
		JedisPool pool = RedisCache.getPool(db);
		if (null == pool || StringUtils.isBlank(key)|| CollectionUtils.isEmpty(info)) {
			return result;
		}

		try (Jedis jedis = pool.getResource()) {
			result = jedis.hmset(key, info);
		} catch (JedisConnectionException connE) {
			connE.printStackTrace();
			try {
				Thread.sleep(1000);
				hmset(db, key, info);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage());
		}
		return result;
	}

	@PostConstruct
	public static void init() {
		log.debug("Init {}", RedisUtil.class.getName());

		String host = LocalConfig.get("spring.redis.host", String.class, "");
		Integer port = LocalConfig.get("spring.redis.port", Integer.class, 0);
		String password = LocalConfig.get("spring.redis.password", String.class, "");

		Map config = new HashMap();
		config.put(RedisConfig.HOST_KEY, host);
		config.put(RedisConfig.PORT_KEY, port);
		config.put(RedisConfig.PASSWORD_KEY, password);
		RedisCache.setPool(0, buildJedisPool(0, config));
	}

	private static JedisPool buildJedisPool(Integer database, Map config) {

		String host = DataUtils.getNotNullValue(config, RedisConfig.HOST_KEY, String.class, "");
		Integer port = DataUtils.getNotNullValue(config, RedisConfig.PORT_KEY, Integer.class, Protocol.DEFAULT_PORT);
		String password = DataUtils.getNotNullValue(config, RedisConfig.PASSWORD_KEY, String.class, "");

		JedisPool pool = new JedisPool();
		if (StringUtils.isNotBlank(host) &&
				StringUtils.isNotBlank(password) &&
				0 < port &&
				0 <= database) {
			pool = new JedisPool(getConfig(), host, port, Protocol.DEFAULT_TIMEOUT, password, database);
			log.debug(pool.getResource().info());
		}
		return pool;
	}

	private static JedisPoolConfig getConfig() {

		if (null == jedisPoolConfig) {
			synchronized (RedisUtil.class) {
				initJedisPoolConfig();
			}
		}

		return jedisPoolConfig;
	}

	private static void initJedisPoolConfig() {
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		// 最大空闲数
		jedisPoolConfig.setMaxIdle(3);
		// 连接池的最大数据库连接数
		jedisPoolConfig.setMaxTotal(1000);
		// 最大建立连接等待时间
		jedisPoolConfig.setMaxWaitMillis(10);
		// 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
		jedisPoolConfig.setMinEvictableIdleTimeMillis(10000);
		// 每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
		jedisPoolConfig.setNumTestsPerEvictionRun(3);
		// 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
		jedisPoolConfig.setTimeBetweenEvictionRunsMillis(3);
		// 是否在从池中取出连接前进行检验,如果检验失败,则从池中去除连接并尝试取出另一个
		jedisPoolConfig.setTestOnBorrow(true);
		// 在空闲时检查有效性, 默认false
		jedisPoolConfig.setTestWhileIdle(true);
		//表示idle object evitor两次扫描之间要sleep的毫秒数
		jedisPoolConfig.setTimeBetweenEvictionRunsMillis(30000);
		//表示idle object evitor每次扫描的最多的对象数
		jedisPoolConfig.setNumTestsPerEvictionRun(10);
		//表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
		jedisPoolConfig.setMinEvictableIdleTimeMillis(60000);
		RedisUtil.jedisPoolConfig = jedisPoolConfig;
	}
}
