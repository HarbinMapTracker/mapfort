package com.mapfort.datacompute.dwsUtils;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
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
public class DriverBehaviorAnalysisTask {

    private final JdbcTemplate jdbcTemplate;

    /** 每天 02:00 触发（CRON 表达式可自行修改）*/
//    @Scheduled(cron = "0 0 2 * * ?")
//    @PostConstruct
//    @Transactional
    public void run() {
        log.info("DWD -> TDM: DriverBehaviorAnalysisTask started.");
        createOrInitTable();
        List<Map<String, Object>> rows = queryDriverStats();
        batchInsert(rows);

        // 输出概要
        double avgTripDur = rows.stream()
                .mapToDouble(r -> ((Number) r.get("avg_trip_duration")).doubleValue())
                .average().orElse(0);
        log.info("DWD -> TDM: DriverBehaviorAnalysisTask finished.");
    }

    /** 创建（若不存在）并清空分析结果表 */
    private void createOrInitTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS public.tdm_driver_behavior_analysis(
                driver_id TEXT PRIMARY KEY,
                total_trips INTEGER,
                avg_trip_duration INTEGER,
                max_trip_duration INTEGER,
                min_trip_duration INTEGER,
                total_driving_time INTEGER,
                morning_trips INTEGER,
                afternoon_trips INTEGER,
                evening_trips INTEGER,
                night_trips INTEGER,
                last_updated TIMESTAMP
            )
            """);
        jdbcTemplate.execute("TRUNCATE TABLE public.tdm_driver_behavior_analysis");
    }

    /** 查询原始业务表得到各项统计 */
    private List<Map<String, Object>> queryDriverStats() {
        String sql = """
            SELECT 
                devid              AS driver_id,
                COUNT(*)           AS total_trips,
                AVG(travel_time)   AS avg_trip_duration,
                MAX(travel_time)   AS max_trip_duration,
                MIN(travel_time)   AS min_trip_duration,
                SUM(travel_time)   AS total_driving_time,
                SUM(CASE WHEN EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN  5 AND 11 THEN 1 ELSE 0 END) AS morning_trips,
                SUM(CASE WHEN EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) AS afternoon_trips,
                SUM(CASE WHEN EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN 18 AND 21 THEN 1 ELSE 0 END) AS evening_trips,
                SUM(CASE WHEN EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN 22 AND 23 
                         OR EXTRACT(HOUR FROM to_timestamp(begin_time)) BETWEEN 0 AND 4 THEN 1 ELSE 0 END) AS night_trips
            FROM dwd_trip_info
            GROUP BY driver_id
            """;
        return jdbcTemplate.queryForList(sql);
    }

    /** 批量写入分析结果 */
    private void batchInsert(List<Map<String, Object>> rows) {
        String insertSql = """
            INSERT INTO public.tdm_driver_behavior_analysis
                (driver_id, total_trips, avg_trip_duration, max_trip_duration, 
                 min_trip_duration, total_driving_time, morning_trips, 
                 afternoon_trips, evening_trips, night_trips, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """;

        jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> r = rows.get(i);
                ps.setString (1,  (String)  r.get("driver_id"));
                ps.setInt    (2,  ((Number) r.get("total_trips")).intValue());
                ps.setInt    (3,  ((Number) r.get("avg_trip_duration")).intValue());
                ps.setInt    (4,  ((Number) r.get("max_trip_duration")).intValue());
                ps.setInt    (5,  ((Number) r.get("min_trip_duration")).intValue());
                ps.setInt    (6,  ((Number) r.get("total_driving_time")).intValue());
                ps.setInt    (7,  ((Number) r.get("morning_trips")).intValue());
                ps.setInt    (8,  ((Number) r.get("afternoon_trips")).intValue());
                ps.setInt    (9,  ((Number) r.get("evening_trips")).intValue());
                ps.setInt    (10, ((Number) r.get("night_trips")).intValue());
                ps.setTimestamp(11, Timestamp.valueOf(LocalDateTime.now()));
            }
            @Override
            public int getBatchSize() {
                return rows.size();
            }
        });
    }
}
