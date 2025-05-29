package com.mapfort.datacompute.dwsUtils;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AreaRoadJoinTask {

    private final JdbcTemplate jdbcTemplate;

    // @Scheduled(cron = "0 0 4 * * ?")
//    @PostConstruct
//    @Transactional
    public void run() {
        log.info("DWD -> TDM: AreaRoadJoinTask started.");
        createOrInitTable();
        insertData();
        log.info("DWD -> TDM: AreaRoadJoinTask finished.");
    }

    private void createOrInitTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS public.tdm_areas_roads (
                node_id BIGINT PRIMARY KEY,
                devid TEXT,
                lon FLOAT,
                lat FLOAT,
                district_code VARCHAR(12),
                district_name VARCHAR(12),
                way_id BIGINT
            )
        """);
        jdbcTemplate.execute("TRUNCATE TABLE public.tdm_areas_roads");
    }

    private void insertData() {
        String sql = """
            INSERT INTO public.tdm_areas_roads 
                (node_id, devid, lon, lat, district_code, district_name, way_id)
            SELECT 
                p.traj_id AS node_id,
                p.devid,
                p.lon,
                p.lat,
                h."PAC" AS district_code,
                h."NAME" AS district_name,
                p.way_id
            FROM public.dwd_pickup_points p
            LEFT JOIN public.harbin_districts h
            ON ST_Within(ST_SetSRID(ST_Point(p.lon, p.lat), 4326), h.geometry)
        """;
        jdbcTemplate.execute(sql);
    }
}
