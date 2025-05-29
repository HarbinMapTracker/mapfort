package com.mapfort.datacompute.tdmUtils;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DriverTripProfileAnalysisTask {

    private final JdbcTemplate jdbcTemplate;

//    @PostConstruct
    public void run() {
        log.info(">>> 开始司机行程画像分析");
        createOrInitTable();
        analyzeAndInsert();
        log.info("<<< 分析完成");
    }

    private void createOrInitTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS public.tdm_driver_trip_profile (
                devid TEXT,
                trip_count INT,
                avg_travel_time FLOAT,
                max_travel_time INT,
                min_travel_time INT,
                total_travel_time BIGINT
            )
        """);
        jdbcTemplate.execute("TRUNCATE TABLE public.tdm_driver_trip_profile");
    }

    private void analyzeAndInsert() {
        jdbcTemplate.execute("""
            INSERT INTO public.tdm_driver_trip_profile
                (devid, trip_count, avg_travel_time, max_travel_time, min_travel_time, total_travel_time)
            SELECT
                devid,
                COUNT(*) AS trip_count,
                AVG(travel_time) AS avg_travel_time,
                MAX(travel_time) AS max_travel_time,
                MIN(travel_time) AS min_travel_time,
                SUM(travel_time) AS total_travel_time
            FROM public.dwd_trip_info
            GROUP BY devid
        """);
    }
}
