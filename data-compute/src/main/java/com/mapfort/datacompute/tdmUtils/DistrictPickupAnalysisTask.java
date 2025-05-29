package com.mapfort.datacompute.tdmUtils;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class DistrictPickupAnalysisTask {

    private final JdbcTemplate jdbcTemplate;

//    @PostConstruct
    public void run() {
        log.info(">>> 区域上车点热度分析启动");
        createSpatialIndex();
        checkDataValidity();
        createAnalysisTable();
        analyzePickupByDistrict();
        log.info("<<< 分析流程结束");
    }

    private void createSpatialIndex() {
        jdbcTemplate.execute("""
            CREATE INDEX IF NOT EXISTS idx_harbin_districts_geometry 
            ON public.harbin_districts USING GIST(geometry)
        """);
    }

    private void checkDataValidity() {
        Map<String, Object> pickupStats = jdbcTemplate.queryForMap("""
            SELECT 
                MIN(lon) AS min_lon, MAX(lon) AS max_lon,
                MIN(lat) AS min_lat, MAX(lat) AS max_lat,
                COUNT(*) AS total_points
            FROM public.dwd_pickup_points
        """);

        Map<String, Object> boundaryStats = jdbcTemplate.queryForMap("""
            SELECT 
                ST_XMin(ST_Envelope(ST_Union(geometry))) AS min_lon,
                ST_XMax(ST_Envelope(ST_Union(geometry))) AS max_lon,
                ST_YMin(ST_Envelope(ST_Union(geometry))) AS min_lat,
                ST_YMax(ST_Envelope(ST_Union(geometry))) AS max_lat
            FROM public.harbin_districts
        """);
    }

    private void createAnalysisTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS public.tdm_district_pickup_analysis_test (
                pac BIGINT PRIMARY KEY,
                district_name TEXT,
                city TEXT,
                province TEXT,
                pickup_count BIGINT
            )
        """);
        jdbcTemplate.execute("TRUNCATE TABLE public.tdm_district_pickup_analysis_test");
    }

    private void analyzePickupByDistrict() {
        jdbcTemplate.execute("""
            INSERT INTO public.tdm_district_pickup_analysis_test (pac, district_name, city, province, pickup_count)
            SELECT 
                d."PAC" AS pac,
                d."NAME" AS district_name,
                d."市" AS city,
                d."省" AS province,
                COUNT(p.traj_id) AS pickup_count
            FROM 
                public.harbin_districts d
            LEFT JOIN 
                public.dwd_pickup_points p
            ON 
                ST_DWithin(ST_SetSRID(ST_Point(p.lon, p.lat), 4326), d.geometry, 0.001)
            GROUP BY 
                d."PAC", d."NAME", d."市", d."省"
        """);

        Integer unmatched = jdbcTemplate.queryForObject("""
            SELECT COUNT(*) FROM public.dwd_pickup_points p
            WHERE NOT EXISTS (
                SELECT 1 FROM public.harbin_districts d
                WHERE ST_DWithin(ST_SetSRID(ST_Point(p.lon, p.lat), 4326), d.geometry, 0.001)
            )
        """, Integer.class);
    }
}
