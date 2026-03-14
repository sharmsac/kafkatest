package com.platform.logging.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component
@Slf4j
public class ArchiveScheduler {

    @Autowired private JdbcTemplate jdbc;

    // ===============================================================
    //  ADD PARTITION -- daily at midnight
    //  Pre-creates partition for 3 days from now
    // ===============================================================
    @Scheduled(cron = "0 0 0 * * *")
    public void addUpcomingPartition() {
        LocalDate future = LocalDate.now().plusDays(3);
        LocalDate next   = future.plusDays(1);
        String partName  = "p_" + future.format(DateTimeFormatter.ofPattern("yyyy_MM_dd"));

        try {
            // TRANSACTION table
            jdbc.execute(
                "ALTER TABLE TRANSACTION REORGANIZE PARTITION p_future INTO ("
                + "PARTITION " + partName + " VALUES LESS THAN (TO_DAYS('" + next + "')), "
                + "PARTITION p_future VALUES LESS THAN MAXVALUE)"
            );

            // TRANSACTION_PAYLOAD table
            jdbc.execute(
                "ALTER TABLE TRANSACTION_PAYLOAD REORGANIZE PARTITION p_future INTO ("
                + "PARTITION " + partName + " VALUES LESS THAN (TO_DAYS('" + next + "')), "
                + "PARTITION p_future VALUES LESS THAN MAXVALUE)"
            );

            log.info("Created partition: {}", partName);
        } catch (Exception e) {
            log.warn("Partition {} may already exist: {}", partName, e.getMessage());
        }
    }

    // ===============================================================
    //  ARCHIVE -- daily at 1am
    //  Moves data older than 7 days -> compressed ARCHIVE_TRANSACTION
    //  Then drops the old partition (instant, no row-by-row delete)
    // ===============================================================
    @Scheduled(cron = "0 0 1 * * *")
    public void archiveOldData() {
        LocalDate archiveDate = LocalDate.now().minusDays(8);
        log.info("Archiving data for: {}", archiveDate);

        // Copy to archive with COMPRESS(JSON)
        jdbc.update("""
            INSERT IGNORE INTO ARCHIVE_TRANSACTION
                (request_id, org_id, org_name, service_name, api_name,
                 total_duration_ms, status_code, data_json, \"date\", `month`)
            SELECT
                request_id, org_id, org_name, service_name, api_name,
                total_duration_ms, status_code,
                COMPRESS(JSON_OBJECT(
                    'apg_request_time',   apg_request_time,
                    'apg_response_time',  apg_response_time,
                    'router_request_time', router_request_time,
                    'router_response_time', router_response_time,
                    'api_request_time',   api_request_time,
                    'api_response_time',  api_response_time,
                    'esp_x_header',       esp_x_header,
                    'is_incomplete',      is_incomplete,
                    'is_error',           is_error,
                    'network_overhead_ms', network_overhead_ms
                )),
                \"date\",
                DATE_FORMAT(\"date\", '%Y-%m-01')
            FROM TRANSACTION
            WHERE \"date\" = ?
            """, archiveDate);

        // Drop partitions (instant operation)
        String partName = "p_" + archiveDate.format(
            DateTimeFormatter.ofPattern("yyyy_MM_dd"));

        try {
            jdbc.execute("ALTER TABLE TRANSACTION DROP PARTITION " + partName);
            jdbc.execute("ALTER TABLE TRANSACTION_PAYLOAD DROP PARTITION " + partName);
            log.info("Dropped partition: {}", partName);
        } catch (Exception e) {
            log.warn("Could not drop partition {}: {}", partName, e.getMessage());
        }
    }
}
