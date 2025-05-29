package com.mapfort.datacompute.dwsUtils;

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
public class PickupHeatmapGridTask {

    private final JdbcTemplate jdbcTemplate;

    // @Scheduled(cron = "0 0 1 * * ?")
//    @PostConstruct@PostConstruct
    @Transactional
    public void run() {
        log.info("DWD -> DWS: PickupHeatmapGridTask started.");
        createOrInitTable();
        List<Map<String, Object>> rows = queryGridStats();
        batchInsert(rows);
        log.info("DWD -> DWS: PickupHeatmapGridTask finished. Inserted: {}", rows.size());
    }

    /** 创建或清空 DWS 表 */
    private void createOrInitTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS public.dws_pickup_grid_stat (
                grid_id TEXT PRIMARY KEY,
                lon_center DOUBLE PRECISION,
                lat_center DOUBLE PRECISION,
                pickup_count INT,
                stat_hour INT,
                stat_date DATE,
                last_updated TIMESTAMP
            )
        """);
        jdbcTemplate.execute("TRUNCATE TABLE public.dws_pickup_grid_stat");
    }

    /** 聚合上车点：按0.001°经纬度栅格划分 */
    private List<Map<String, Object>> queryGridStats() {
        String sql = """
            SELECT
              md5(floor(lon / 0.001) || '_' || floor(lat / 0.001) || '_' || to_timestamp(tms)::date || '_' || EXTRACT(HOUR FROM to_timestamp(tms))) AS grid_id,
              floor(lon / 0.001) * 0.001 + 0.0005 AS lon_center,
              floor(lat / 0.001) * 0.001 + 0.0005 AS lat_center,
              COUNT(*) AS pickup_count,
              EXTRACT(HOUR FROM to_timestamp(tms)) AS stat_hour,
              to_timestamp(tms)::date AS stat_date
            FROM dwd_pickup_points
            GROUP BY grid_id, lon_center, lat_center, stat_hour, stat_date
        """;
        return jdbcTemplate.queryForList(sql);
    }

    /** 批量写入栅格聚合结果 */
    private void batchInsert(List<Map<String, Object>> rows) {
        String insertSql = """
            INSERT INTO public.dws_pickup_grid_stat
                (grid_id, lon_center, lat_center, pickup_count, stat_hour, stat_date, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

        jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> r = rows.get(i);
                ps.setString(1, (String) r.get("grid_id"));
                ps.setDouble(2, ((Number) r.get("lon_center")).doubleValue());
                ps.setDouble(3, ((Number) r.get("lat_center")).doubleValue());
                ps.setInt(4, ((Number) r.get("pickup_count")).intValue());
                ps.setInt(5, ((Number) r.get("stat_hour")).intValue());
                ps.setDate(6, java.sql.Date.valueOf(r.get("stat_date").toString()));
                ps.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
            }

            @Override
            public int getBatchSize() {
                return rows.size();
            }
        });
    }
}
