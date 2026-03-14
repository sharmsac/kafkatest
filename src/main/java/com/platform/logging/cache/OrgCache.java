package com.platform.logging.cache;

import com.google.common.cache.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class OrgCache {

    @Data
    @AllArgsConstructor
    public static class OrgInfo {
        private Long   id;
        private String name;
    }

    @Autowired private JdbcTemplate jdbc;

    // -- Lookup by org ID --
    private LoadingCache<Long, OrgInfo> byId;

    // -- Lookup name -> ID --
    private LoadingCache<String, Long> nameToId;

    @PostConstruct
    public void init() {
        byId = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .refreshAfterWrite(10, TimeUnit.MINUTES)
            .build(new CacheLoader<Long, OrgInfo>() {
                @Override
                public OrgInfo load(Long orgId) {
                    return jdbc.queryForObject(
                        "SELECT id, name FROM ORG WHERE id = ?",
                        (rs, row) -> new OrgInfo(rs.getLong("id"), rs.getString("name")),
                        orgId
                    );
                }
            });

        nameToId = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .refreshAfterWrite(10, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Long>() {
                @Override
                public Long load(String name) {
                    return jdbc.queryForObject(
                        "SELECT id FROM ORG WHERE name = ?",
                        Long.class, name
                    );
                }
            });
    }

    public OrgInfo getById(Long orgId) {
        try { return byId.get(orgId); }
        catch (Exception e) { return null; }
    }

    public Long resolveNameToId(String name) {
        try { return nameToId.get(name); }
        catch (Exception e) { return null; }
    }

    // -- Listen for org config changes -> backfill denormalized org_name --
    @EventListener
    public void onOrgConfigChanged(OrgConfigChangedEvent event) {
        log.info("Org config changed: {} -> {}", event.getOrgId(), event.getNewName());

        // Invalidate caches
        byId.invalidate(event.getOrgId());
        if (event.getOldName() != null) nameToId.invalidate(event.getOldName());

        // Backfill org_name on historical TRANSACTION records
        int updated = jdbc.update(
            "UPDATE TRANSACTION SET org_name = ? WHERE org_id = ? AND org_name != ?",
            event.getNewName(), event.getOrgId(), event.getNewName()
        );
        log.info("Backfilled org_name for {} rows", updated);
    }
}
