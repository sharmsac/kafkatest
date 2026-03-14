package com.platform.logging.loadtest;

import com.platform.logging.consumer.FlushSchedulers;
import com.platform.logging.consumer.TransactionAssemblyConsumer;
import com.platform.logging.model.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("loadtest")
@Import(LoadTestConfig.class)
@EmbeddedKafka(
    partitions = 30,
    topics = {"load-test-events", "load-test-dlq"},
    brokerProperties = {"log.dir=target/embedded-kafka-logs"}
)
@DirtiesContext
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LoadTest100K {

    private static final int TOTAL_TRANSACTIONS = 100_000;
    private static final int PRODUCER_THREADS = 10;
    private static final String TOPIC = "load-test-events";

    @Autowired private EmbeddedKafkaBroker embeddedKafka;
    @Autowired private TransactionAssemblyConsumer consumer;
    @Autowired private FlushSchedulers flushSchedulers;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private KafkaListenerEndpointRegistry registry;
    @Autowired private KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer c : registry.getListenerContainers()) {
            try { ContainerTestUtils.waitForAssignment(c, embeddedKafka.getPartitionsPerTopic()); }
            catch (Exception ignored) {}
        }
    }

    @Test
    @Order(1)
    @DisplayName("Load test: 100K transactions through full pipeline")
    void loadTest100K() throws Exception {

        System.out.println("\n" + "=".repeat(70));
        System.out.println("  LOAD TEST: " + TOTAL_TRANSACTIONS + " transactions");
        System.out.println("  Real flow: APG_REQ → ROUTER_REQ → API_CALL (flush!)");
        System.out.println("             then ROUTER_RESP + APG_RESP arrive as late updates");
        System.out.println("=".repeat(70));

        Instant totalStart = Instant.now();

        // ─── PHASE 1: APG_REQUEST + ROUTER_REQUEST + API_CALL ───
        // API_CALL triggers flush. APG/Router requests merged before flush.
        System.out.println("\n  [Phase 1] APG_REQUEST + ROUTER_REQUEST + API_CALL (3 events each)");
        Instant p1Start = Instant.now();
        AtomicInteger sent = new AtomicInteger(0);

        sendInParallel((start, end) -> {
            for (int i = start; i < end; i++) {
                String reqId = reqId(i);
                long orgId = (i % 100) + 1;
                long now = System.currentTimeMillis();
                int dur = 50 + (i % 950);
                int status = (i % 20 == 0) ? 500 : 200;

                // APG_REQUEST
                kafkaTemplate.send(TOPIC, reqId, TransactionEvent.builder()
                    .requestId(reqId).orgId(orgId).orgName("Org-" + orgId)
                    .serviceName("Service-" + (i % 25)).apiName("api-" + (i % 5))
                    .eventType(EventType.APG_REQUEST).startTime(now)
                    .publishedAt(now).build());

                // ROUTER_REQUEST
                kafkaTemplate.send(TOPIC, reqId, TransactionEvent.builder()
                    .requestId(reqId).eventType(EventType.ROUTER_REQUEST)
                    .startTime(now + 5).publishedAt(now).build());

                // API_CALL → triggers flush
                kafkaTemplate.send(TOPIC, reqId, TransactionEvent.builder()
                    .requestId(reqId).eventType(EventType.API_CALL)
                    .startTime(now + 10).endTime(now + 10 + dur).statusCode(status)
                    .publishedAt(now).build());

                int c = sent.incrementAndGet();
                if (c % 25_000 == 0)
                    System.out.printf("    Produced: %,d / %,d%n", c, TOTAL_TRANSACTIONS);
            }
        });
        Duration p1Dur = Duration.between(p1Start, Instant.now());
        System.out.printf("    Phase 1 produce: %s (%,.0f events/sec)%n",
            p1Dur, TOTAL_TRANSACTIONS * 3.0 / (p1Dur.toMillis() / 1000.0));

        // Wait for flushes
        System.out.println("\n  [Waiting] Assembly + hot flush...");
        Instant consumeStart = Instant.now();
        waitForDbCount(TOTAL_TRANSACTIONS * 0.95, 90);
        Duration consumeDur = Duration.between(consumeStart, Instant.now());

        // ─── PHASE 2: Late arrivals — ROUTER_RESPONSE + APG_RESPONSE ───
        System.out.println("\n  [Phase 2] ROUTER_RESPONSE + APG_RESPONSE (late arrivals, 2 events each)");
        Instant p2Start = Instant.now();
        sent.set(0);

        sendInParallel((start, end) -> {
            for (int i = start; i < end; i++) {
                String reqId = reqId(i);
                long now = System.currentTimeMillis();
                int dur = 50 + (i % 950);

                // ROUTER_RESPONSE (late)
                kafkaTemplate.send(TOPIC, reqId, TransactionEvent.builder()
                    .requestId(reqId).eventType(EventType.ROUTER_RESPONSE)
                    .endTime(now + dur - 5).publishedAt(now).build());

                // APG_RESPONSE (late)
                kafkaTemplate.send(TOPIC, reqId, TransactionEvent.builder()
                    .requestId(reqId).eventType(EventType.APG_RESPONSE)
                    .endTime(now + dur).publishedAt(now).build());

                int c = sent.incrementAndGet();
                if (c % 25_000 == 0)
                    System.out.printf("    Produced: %,d / %,d%n", c, TOTAL_TRANSACTIONS);
            }
        });
        Duration p2Dur = Duration.between(p2Start, Instant.now());
        System.out.printf("    Phase 2 produce: %s (%,.0f events/sec)%n",
            p2Dur, TOTAL_TRANSACTIONS * 2.0 / (p2Dur.toMillis() / 1000.0));

        // ─── PHASE 3: CORE_PAYLOAD for errors ───
        System.out.println("\n  [Phase 3] CORE_PAYLOAD for error transactions (5%)");
        Instant p3Start = Instant.now();
        int payloadsSent = 0;
        for (int i = 0; i < TOTAL_TRANSACTIONS; i++) {
            if (i % 20 == 0) {
                kafkaTemplate.send(TOPIC, reqId(i), TransactionEvent.builder()
                    .requestId(reqId(i)).eventType(EventType.CORE_PAYLOAD)
                    .data("{\"error\":\"Internal Server Error\",\"reqId\":\"" + reqId(i) + "\"}")
                    .publishedAt(System.currentTimeMillis()).build());
                payloadsSent++;
            }
        }
        Duration p3Dur = Duration.between(p3Start, Instant.now());
        System.out.printf("    Phase 3: %,d payloads in %s%n", payloadsSent, p3Dur);

        // Wait for everything
        System.out.println("\n  [Waiting] Final settle (late arrivals + flush)...");
        Instant lateArrivalStart = Instant.now();
        Thread.sleep(5000); // let late arrivals + flush complete
        Duration lateArrivalDur = Duration.between(lateArrivalStart, Instant.now());

        Duration totalDur = Duration.between(totalStart, Instant.now());

        // ─── RESULTS ───
        printResults(totalDur, p1Dur, p2Dur, p3Dur, consumeDur, lateArrivalDur);
    }

    @Test
    @Order(2)
    @DisplayName("Query performance benchmark on 100K rows")
    void queryPerformance() {
        int total = jdbc.queryForObject("SELECT COUNT(*) FROM TRANSACTION", Integer.class);
        if (total == 0) { System.out.println("  Skip — no data"); return; }

        System.out.println("\n" + "=".repeat(70));
        System.out.printf("  QUERY PERFORMANCE on %,d rows%n", total);
        System.out.println("=".repeat(70));

        bench("org + date filter",
            "SELECT * FROM TRANSACTION WHERE org_id = 1 AND \"date\" = CURRENT_DATE LIMIT 50");
        bench("errors only",
            "SELECT * FROM TRANSACTION WHERE org_id = 1 AND is_error = TRUE LIMIT 50");
        bench("slow calls (>500ms)",
            "SELECT * FROM TRANSACTION WHERE total_duration_ms > 500 LIMIT 50");
        bench("top 10 orgs",
            "SELECT org_id, COUNT(*) cnt, AVG(total_duration_ms) avg FROM TRANSACTION GROUP BY org_id ORDER BY cnt DESC LIMIT 10");
        bench("request_id lookup",
            "SELECT * FROM TRANSACTION WHERE request_id = 'req-000042'");
        bench("count all",
            "SELECT COUNT(*) FROM TRANSACTION");

        System.out.println("=".repeat(70));
    }

    // =====================================================================

    private String reqId(int i) { return "req-" + String.format("%06d", i); }

    @FunctionalInterface
    interface RangeTask { void run(int start, int end); }

    private void sendInParallel(RangeTask task) throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(PRODUCER_THREADS);
        int batch = TOTAL_TRANSACTIONS / PRODUCER_THREADS;
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < PRODUCER_THREADS; t++) {
            int s = t * batch, e = s + batch;
            futures.add(exec.submit(() -> task.run(s, e)));
        }
        for (Future<?> f : futures) f.get(120, TimeUnit.SECONDS);
        exec.shutdown();
    }

    private void waitForDbCount(double target, int timeoutSec) throws Exception {
        Instant waitStart = Instant.now();
        int lastCount = 0;
        long lastPrintMs = 0;

        for (int i = 0; i < timeoutSec * 2; i++) {
            Thread.sleep(500);
            int count = jdbc.queryForObject("SELECT COUNT(*) FROM TRANSACTION", Integer.class);
            int asm = consumer.getAssemblyBuffer().size();
            int hot = consumer.getHotBuffer().size();
            int cold = consumer.getColdBuffer().size();
            int payload = consumer.getPayloadBuffer().size();
            long flushed = flushSchedulers.getHotFlushRecords().get()
                         + flushSchedulers.getColdFlushRecords().get();

            long elapsedMs = Duration.between(waitStart, Instant.now()).toMillis();

            // Print every 2 seconds
            if (elapsedMs - lastPrintMs >= 2000 || i == 0) {
                double deltaSec = lastPrintMs == 0 ? 1.0 : (elapsedMs - lastPrintMs) / 1000.0;
                int deltaRows = count - lastCount;
                double rowsPerSec = deltaSec > 0 ? deltaRows / deltaSec : 0;

                System.out.printf("    [%.1fs] DB=%,d  asm=%,d  hot=%,d  cold=%,d  payload=%,d  flushed=%,d  (+%,.0f rows/sec)%n",
                    elapsedMs / 1000.0, count, asm, hot, cold, payload, flushed, rowsPerSec);

                lastCount = count;
                lastPrintMs = elapsedMs;
            }

            if (count >= target && asm == 0 && hot == 0 && cold == 0) return;
        }
    }

    private void printResults(Duration total, Duration p1, Duration p2, Duration p3,
                              Duration consumeDur, Duration lateArrivalDur) {
        int dbRows = jdbc.queryForObject("SELECT COUNT(*) FROM TRANSACTION", Integer.class);
        int payloads = jdbc.queryForObject("SELECT COUNT(*) FROM TRANSACTION_PAYLOAD", Integer.class);
        int errors = jdbc.queryForObject("SELECT COUNT(*) FROM TRANSACTION WHERE is_error = TRUE", Integer.class);
        int incomplete = jdbc.queryForObject("SELECT COUNT(*) FROM TRANSACTION WHERE is_incomplete = TRUE", Integer.class);
        int withRouter = jdbc.queryForObject("SELECT COUNT(*) FROM TRANSACTION WHERE has_router = TRUE", Integer.class);

        Double avgDur = jdbc.queryForObject(
            "SELECT AVG(total_duration_ms) FROM TRANSACTION WHERE total_duration_ms IS NOT NULL", Double.class);
        int orgs = jdbc.queryForObject("SELECT COUNT(DISTINCT org_id) FROM TRANSACTION", Integer.class);
        int svcs = jdbc.queryForObject("SELECT COUNT(DISTINCT service_name) FROM TRANSACTION", Integer.class);

        int totalEvents = TOTAL_TRANSACTIONS * 5 + (TOTAL_TRANSACTIONS / 20); // 5 events + payload for 5%
        double eventsPerSec = totalEvents / (total.toMillis() / 1000.0);
        double txnPerSec = dbRows / (total.toMillis() / 1000.0);

        // Read flush counters
        long hotCount   = flushSchedulers.getHotFlushCount().get();
        long hotTimeMs  = flushSchedulers.getHotFlushTimeMs().get();
        long hotRecords = flushSchedulers.getHotFlushRecords().get();
        long coldCount   = flushSchedulers.getColdFlushCount().get();
        long coldTimeMs  = flushSchedulers.getColdFlushTimeMs().get();
        long coldRecords = flushSchedulers.getColdFlushRecords().get();
        long plCount   = flushSchedulers.getPayloadFlushCount().get();
        long plTimeMs  = flushSchedulers.getPayloadFlushTimeMs().get();
        long plRecords = flushSchedulers.getPayloadFlushRecords().get();
        long retries   = flushSchedulers.getBatchRetryCount().get();
        long dlq       = flushSchedulers.getDlqCount().get();

        System.out.println("\n" + "=".repeat(70));
        System.out.println("  LOAD TEST RESULTS");
        System.out.println("=".repeat(70));

        // ── KAFKA PRODUCE ──
        System.out.println("\n  KAFKA PRODUCE");
        System.out.printf("    Phase 1 (APG+Router+API):     %s  (%,.0f events/sec)%n",
            p1, TOTAL_TRANSACTIONS * 3.0 / (p1.toMillis() / 1000.0));
        System.out.printf("    Phase 2 (late Router+APG):    %s  (%,.0f events/sec)%n",
            p2, TOTAL_TRANSACTIONS * 2.0 / (p2.toMillis() / 1000.0));
        System.out.printf("    Phase 3 (error payloads):     %s%n", p3);
        System.out.printf("    Total Kafka events:           %,d%n", totalEvents);

        // ── KAFKA CONSUME ──
        System.out.println("\n  KAFKA CONSUME");
        System.out.printf("    Consumer drain time (P1):     %s%n", consumeDur);
        System.out.printf("    Late arrival settle (P2+P3):  %s%n", lateArrivalDur);
        System.out.printf("    Events/sec (end-to-end):      %,.0f%n", eventsPerSec);
        System.out.printf("    Transactions/sec:             %,.0f%n", txnPerSec);
        System.out.printf("    Total end-to-end:             %s%n", total);

        // ── MYSQL WRITES ──
        System.out.println("\n  MYSQL WRITES");
        System.out.println("    Hot flush (INSERT):");
        System.out.printf("      Flushes:        %,d%n", hotCount);
        System.out.printf("      Records:        %,d%n", hotRecords);
        System.out.printf("      Total time:     %,d ms%n", hotTimeMs);
        System.out.printf("      Avg batch size: %,.0f%n", hotCount > 0 ? (double) hotRecords / hotCount : 0);
        System.out.printf("      Avg flush time: %,.1f ms%n", hotCount > 0 ? (double) hotTimeMs / hotCount : 0);
        System.out.println("    Cold flush (INSERT):");
        System.out.printf("      Flushes:        %,d%n", coldCount);
        System.out.printf("      Records:        %,d%n", coldRecords);
        System.out.printf("      Total time:     %,d ms%n", coldTimeMs);
        System.out.printf("      Avg batch size: %,.0f%n", coldCount > 0 ? (double) coldRecords / coldCount : 0);
        System.out.printf("      Avg flush time: %,.1f ms%n", coldCount > 0 ? (double) coldTimeMs / coldCount : 0);
        System.out.println("    Payload flush (INSERT):");
        System.out.printf("      Flushes:        %,d%n", plCount);
        System.out.printf("      Records:        %,d%n", plRecords);
        System.out.printf("      Total time:     %,d ms%n", plTimeMs);
        System.out.printf("      Avg batch size: %,.0f%n", plCount > 0 ? (double) plRecords / plCount : 0);
        System.out.printf("      Avg flush time: %,.1f ms%n", plCount > 0 ? (double) plTimeMs / plCount : 0);
        System.out.println("    Late arrival UPDATEs:");
        System.out.printf("      Rows with router (updated): %,d%n", withRouter);

        // ── FAILURES ──
        System.out.println("\n  FAILURES");
        System.out.printf("    Batch retries:    %,d%n", retries);
        System.out.printf("    DLQ sends:        %,d%n", dlq);

        // ── DATA INTEGRITY ──
        System.out.println("\n  DATA INTEGRITY");
        System.out.printf("    DB rows:          %,d / %,d (%.1f%%)%n",
            dbRows, TOTAL_TRANSACTIONS, dbRows * 100.0 / TOTAL_TRANSACTIONS);
        System.out.printf("    Complete:         %,d%n", dbRows - incomplete);
        System.out.printf("    Incomplete:       %,d%n", incomplete);
        System.out.printf("    With router:      %,d%n", withRouter);
        System.out.printf("    Errors (5%%):      %,d (%.1f%%)%n", errors, errors * 100.0 / Math.max(1, dbRows));
        System.out.printf("    Payloads in DB:   %,d%n", payloads);
        System.out.printf("    Distinct orgs:    %d%n", orgs);
        System.out.printf("    Distinct svcs:    %d%n", svcs);
        System.out.printf("    Avg duration:     %.0f ms%n", avgDur != null ? avgDur : 0);

        // ── END-TO-END LATENCY (event created → written to DB) ──
        long latCount = flushSchedulers.getLatencyCount().get();
        long latTotal = flushSchedulers.getTotalLatencyMs().get();
        long latMin   = flushSchedulers.getMinLatencyMs().get();
        long latMax   = flushSchedulers.getMaxLatencyMs().get();

        System.out.println("\n  END-TO-END LATENCY (event published → written to DB)");
        if (latCount > 0) {
            System.out.printf("    Min:              %,d ms%n", latMin == Long.MAX_VALUE ? 0 : latMin);
            System.out.printf("    Avg:              %,d ms%n", latTotal / latCount);
            System.out.printf("    Max:              %,d ms%n", latMax);
            System.out.printf("    Samples:          %,d%n", latCount);
        } else {
            System.out.println("    (no latency data)");
        }

        // ── QUERY PERFORMANCE ──
        System.out.println("\n  QUERY PERFORMANCE");
        bench("org + date filter",
            "SELECT * FROM TRANSACTION WHERE org_id = 1 AND \"date\" = CURRENT_DATE LIMIT 50");
        bench("errors only",
            "SELECT * FROM TRANSACTION WHERE org_id = 1 AND is_error = TRUE LIMIT 50");
        bench("slow calls (>500ms)",
            "SELECT * FROM TRANSACTION WHERE total_duration_ms > 500 LIMIT 50");
        bench("top 10 orgs",
            "SELECT org_id, COUNT(*) cnt, AVG(total_duration_ms) avg FROM TRANSACTION GROUP BY org_id ORDER BY cnt DESC LIMIT 10");
        bench("request_id lookup",
            "SELECT * FROM TRANSACTION WHERE request_id = 'req-000042'");
        bench("count all",
            "SELECT COUNT(*) FROM TRANSACTION");

        System.out.println("\n" + "=".repeat(70));

        // Assertions
        assertTrue(dbRows >= TOTAL_TRANSACTIONS * 0.99,
            "Expected >=99%% in DB, got " + dbRows);
        assertTrue(errors > 0, "Should have errors");
        double errPct = errors * 100.0 / dbRows;
        assertTrue(errPct > 3 && errPct < 7, "Error rate ~5%, got " + errPct + "%");

        System.out.println("  ALL ASSERTIONS PASSED");
    }

    private void bench(String label, String sql) {
        Instant s = Instant.now();
        var r = jdbc.queryForList(sql);
        long ms = Duration.between(s, Instant.now()).toMillis();
        System.out.printf("    %-22s %,d rows in %d ms%n", label, r.size(), ms);
    }
}
