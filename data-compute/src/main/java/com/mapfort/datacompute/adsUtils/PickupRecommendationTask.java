package com.mapfort.datacompute.adsUtils;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@EnableScheduling
@RequiredArgsConstructor
public class PickupRecommendationTask {

    private final JdbcTemplate jdbcTemplate;

    @PostConstruct
    @Transactional
    public void run() {
        log.info("DWS -> ADS: PickupRecommendationTask started.");
        createOrInitTable();
        List<Map<String, Object>> data = queryPickupRecommendations();
        batchInsert(data);
        log.info("DWS -> ADS: PickupRecommendationTask finished. Inserted: {}", data.size());
    }

    private void createOrInitTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS public.ads_recommend_pickup_ways (
                is_workday BOOLEAN,
                stat_hour INT,
                way_name TEXT,
                district_name TEXT,
                pickup_count INT,
                last_updated TIMESTAMP,
                PRIMARY KEY (is_workday, stat_hour, way_name, district_name)
            )
        """);
        jdbcTemplate.execute("TRUNCATE TABLE public.ads_recommend_pickup_ways");
    }

    private List<Map<String, Object>> queryPickupRecommendations() {
        return jdbcTemplate.queryForList("""
            SELECT
                EXTRACT(DOW FROM TO_TIMESTAMP(p.tms)) BETWEEN 1 AND 5 AS is_workday,
                EXTRACT(HOUR FROM TO_TIMESTAMP(p.tms))::INT AS stat_hour,
                r.tags::jsonb ->> 'name' AS way_name,
                d."NAME" AS district_name,
                COUNT(*) AS pickup_count
            FROM
                dwd_pickup_points p
            JOIN harbin_districts d
                ON ST_Within(ST_SetSRID(ST_Point(p.lon, p.lat), 4326), d.geometry)
            JOIN LATERAL (
                SELECT *
                FROM dwd_roads r
                WHERE r.tags::jsonb ? 'name'
                  AND ST_DWithin(ST_SetSRID(ST_Point(p.lon, p.lat), 4326), r.geom, 0.0005)
                ORDER BY ST_Distance(ST_SetSRID(ST_Point(p.lon, p.lat), 4326), r.geom)
                LIMIT 1
            ) r ON true
            GROUP BY is_workday, stat_hour, way_name, district_name
        """);
    }

    private void batchInsert(List<Map<String, Object>> rows) {
        String sql = """
            INSERT INTO public.ads_recommend_pickup_ways
                (is_workday, stat_hour, way_name, district_name, pickup_count, last_updated)
            VALUES (?, ?, ?, ?, ?, ?)
        """;

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> row = rows.get(i);
                ps.setBoolean(1, (Boolean) row.get("is_workday"));
                ps.setInt(2, ((Number) row.get("stat_hour")).intValue());
                ps.setString(3, (String) row.get("way_name"));
                ps.setString(4, (String) row.get("district_name"));
                ps.setInt(5, ((Number) row.get("pickup_count")).intValue());
                ps.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now()));
            }

            @Override
            public int getBatchSize() {
                return rows.size();
            }
        });
    }
}
