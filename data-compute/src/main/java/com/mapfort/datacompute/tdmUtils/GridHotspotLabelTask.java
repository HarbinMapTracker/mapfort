package com.mapfort.datacompute.tdmUtils;

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
public class GridHotspotLabelTask {

    private final JdbcTemplate jdbcTemplate;

//    @PostConstruct
//    @Transactional
    public void run() {
        log.info("DWS -> TDM: GridHotspotLabelTask started.");
        createOrInitTable();
        List<Map<String, Object>> rows = queryWithQuantiles();
        batchInsert(rows);
        log.info("DWS -> TDM: GridHotspotLabelTask finished. Inserted: {}", rows.size());
    }

    private void createOrInitTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS public.tdm_hot_grids (
                grid_id TEXT,
                heat_level TEXT,
                pickup_count INT,
                lon_center DOUBLE PRECISION,
                lat_center DOUBLE PRECISION,
                stat_hour INT,
                is_workday BOOLEAN,
                last_updated TIMESTAMP,
                PRIMARY KEY (grid_id, stat_hour, is_workday)
            )
        """);
        jdbcTemplate.execute("TRUNCATE TABLE public.tdm_hot_grids");
    }

    private List<Map<String, Object>> queryWithQuantiles() {
        return jdbcTemplate.queryForList("""
        WITH q AS (
          SELECT 
            percentile_cont(0.75) WITHIN GROUP (ORDER BY pickup_count) AS q75,
            percentile_cont(0.5)  WITHIN GROUP (ORDER BY pickup_count) AS q50
          FROM public.dws_pickup_grid_stat
        )
        SELECT 
          g.grid_id,
          g.pickup_count,
          g.lon_center,
          g.lat_center,
          g.stat_hour,
          -- 判断是否工作日：周一~周五为 TRUE，周六日为 FALSE
          (EXTRACT(DOW FROM g.stat_date)::INT BETWEEN 1 AND 5) AS is_workday,
          CASE 
            WHEN g.pickup_count >= q.q75 THEN '高'
            WHEN g.pickup_count >= q.q50 THEN '中'
            ELSE '低'
          END AS heat_level
        FROM public.dws_pickup_grid_stat g, q
    """);
    }

    private void batchInsert(List<Map<String, Object>> rows) {
        String sql = """
            INSERT INTO public.tdm_hot_grids
                (grid_id, heat_level, pickup_count, lon_center, lat_center, stat_hour, is_workday, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """;

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> row = rows.get(i);
                ps.setString(1, (String) row.get("grid_id"));
                ps.setString(2, (String) row.get("heat_level"));
                ps.setInt(3, ((Number) row.get("pickup_count")).intValue());
                ps.setDouble(4, ((Number) row.get("lon_center")).doubleValue());
                ps.setDouble(5, ((Number) row.get("lat_center")).doubleValue());
                ps.setInt(6, ((Number) row.get("stat_hour")).intValue());
                ps.setBoolean(7, (Boolean) row.get("is_workday"));
                ps.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
            }

            public int getBatchSize() {
                return rows.size();
            }
        });
    }
}
