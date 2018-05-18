package fiddle.redisperf;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Here we will compare different ways to call two Redis commands together.
 * The results will be heavily influenced by connection latency, so we will
 * measure 3 types of connections: local socket, local TCP, remote TCP.
 * Our contenders are @Benchmark methods. They will be described in their own
 * javadoc.
 * @author imaskar
 */
@State(Scope.Benchmark)
public class RedisBatchesBenchmark {

  public static final String KEY = "test";
  public static final String SCRIPT_BODY = "local val=redis.call(\"incrby\",KEYS[1],KEYS[2]) redis.call(\"expire\",KEYS[1],KEYS[3]) return val";
  public static final String SCRIPT_BODY_GETSET = "local val=redis.call(\"get\",KEYS[1]) val=val+KEYS[2] redis.call(\"setex\",KEYS[1],KEYS[3],val) return val";

  @State(Scope.Benchmark)
  public static class RedisState {

    public RedisClient client;
    public StatefulRedisConnection<String, String> conn;
    public String sha;

    public void init(RedisURI uri) {
      if (client != null) {
        client.shutdown();
      }
      client = RedisClient.create(uri);
      conn = client.connect();

    }

    @Setup(Level.Trial)
    public void setup(BenchmarkParams params) throws InterruptedException, ExecutionException {
      RedisURI uri;
      switch (address) {
        case "socket":
          uri = RedisURI.Builder
              .socket("/var/lib/redis/redis6370.sock")
              .build();
          break;
        case "localhost":
          uri = RedisURI.Builder
              .redis("127.0.0.1", 6370)
              .build();
          break;
        case "remote":
          uri = RedisURI.Builder
              .redis("192.168.61.245", 6370)
              .build();
          break;
        default:
          throw new IllegalArgumentException(address);
      }
      init(uri);
      conn.sync().set(KEY, "0");
      if ("fiddle.redisperf.RedisBatchesBenchmark.async_flush".equals(params.getBenchmark())) {
        conn.setAutoFlushCommands(false); //only for async
        System.out.println("AutoFlush: off");
      }
      if ("fiddle.redisperf.RedisBatchesBenchmark.evalsha".equals(params.getBenchmark())) {
        sha = conn.sync().scriptLoad(SCRIPT_BODY);
        System.out.println("Script loaded, SHA = " + sha);
      }
      if ("fiddle.redisperf.RedisBatchesBenchmark.evalshags".equals(params.getBenchmark())) {
        sha = conn.sync().scriptLoad(SCRIPT_BODY_GETSET);
        System.out.println("Script loaded, SHA = " + sha);
      }

    }

  }

  @Param({"socket", "localhost", "remote"})
  public static String address;

  /**
   * The simplest way when we don't need atomicity.
   * @param state
   * @return 
   */
  @Benchmark
  public long naive(RedisState state) {
    final RedisCommands<String, String> sync = state.conn.sync();
    Long incrby = sync.incrby(KEY, 7);
    sync.expire(KEY, 600);
    return incrby;
  }

  /**
   * No atomicity again, but here we use the fact, that we don't need to wait
   * for result of the second command.
   * @param state
   * @return
   * @throws InterruptedException
   * @throws ExecutionException 
   */
  @Benchmark
  public long async(RedisState state) throws InterruptedException, ExecutionException {
    RedisAsyncCommands<String, String> async = state.conn.async();
    RedisFuture<Long> incrby = async.incrby(KEY, 7);
    async.expire(KEY, 600);
    return incrby.get();
  }

  /**
   * No atomicity, not waiting for EXPIRE and sending both commands together.
   * @param state
   * @return
   * @throws InterruptedException
   * @throws ExecutionException 
   */
  @Benchmark
  public long async_flush(RedisState state) throws InterruptedException, ExecutionException {
    RedisAsyncCommands<String, String> async = state.conn.async();
    RedisFuture<Long> incrby = async.incrby(KEY, 7);
    async.expire(KEY, 600);
    async.flushCommands();
    return incrby.get();
  }

  /**
   * Atomically call commands via MULTI. Here we actually have unnecessary
   * overhead, but let's keep it for completeness.
   * @param state
   * @return 
   */
  @Benchmark
  public long multi(RedisState state) {
    final RedisCommands<String, String> sync = state.conn.sync();
    sync.multi();
    sync.incrby(KEY, 7);
    sync.expire(KEY, 600);
    TransactionResult exec = sync.exec();
    return exec.get(0);
  }
  /**
   * Atomically do both actions via EVAL. Here we send script body over and 
   * over again.
   * @param state
   * @return 
   */
  @Benchmark
  public long eval(RedisState state) {
    final RedisCommands<String, String> sync = state.conn.sync();
    Long eval = sync.eval(SCRIPT_BODY, ScriptOutputType.INTEGER,/*new String[]{"k","i","e"},*/ KEY, "7", "600");
    return eval;
  }

  /**
   * Atomically do both actions via EVAL. Here we save script's digest and call
   * it by that. Note, that server will make a lookup in scripts table, so 
   * results may degrade if you have a lot of scripts.
   * @param state
   * @return 
   */
  @Benchmark
  public long evalsha(RedisState state) {
    final RedisCommands<String, String> sync = state.conn.sync();
    Long eval = sync.evalsha(state.sha, ScriptOutputType.INTEGER,/*new String[]{"k","i","e"},*/ KEY, "7", "600");
    return eval;
  }

  /**
   * Atomically do both actions via EVAL. Here we send script body over and 
   * over again. But here we user other commands for doing the same. Just 
   * because we can.
   * @param state
   * @return 
   */
  @Benchmark
  public long evalgs(RedisState state) {
    final RedisCommands<String, String> sync = state.conn.sync();
    Long eval = sync.eval(SCRIPT_BODY_GETSET, ScriptOutputType.INTEGER,/*new String[]{"k","i","e"},*/ KEY, "7", "600");
    return eval;
  }

    /**
   * Atomically do both actions via EVAL. Here we save script's digest and call
   * it by that. But here we user other commands for doing the same. Just 
   * because we can.
   * @param state
   * @return 
   */
  @Benchmark
  public long evalshags(RedisState state) {
    final RedisCommands<String, String> sync = state.conn.sync();
    Long eval = sync.evalsha(state.sha, ScriptOutputType.INTEGER,/*new String[]{"k","i","e"},*/ KEY, "7", "600");
    return eval;
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(RedisBatchesBenchmark.class.getSimpleName())
        .timeUnit(TimeUnit.MICROSECONDS)
        .warmupTime(TimeValue.seconds(5))
        .warmupIterations(2)
        .measurementTime(TimeValue.seconds(10))
        .measurementIterations(5)
        .forks(5)
        .threads(1)
        .mode(Mode.Throughput)
        .shouldFailOnError(true)
        .shouldDoGC(true)
        .build();

    new Runner(opt).run();
  }
}
