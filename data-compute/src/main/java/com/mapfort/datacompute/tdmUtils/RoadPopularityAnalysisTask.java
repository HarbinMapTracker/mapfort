package com.mapfort.datacompute.tdmUtils;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@EnableScheduling
@RequiredArgsConstructor
public class RoadPopularityAnalysisTask {

    private final JdbcTemplate jdbcTemplate;

    // @Scheduled(cron = "0 0 3 * * ?")
//    @PostConstruct
    public void run() {
        log.info("DWD -> TDM: RoadPopularityAnalysisTask started.");
        createOrInitTable();
        List<RoadStats> roads = fetchWayFrequencies();
        assignPopularityTags(roads);
        batchInsert(roads);
        log.info("DWD -> TDM: RoadPopularityAnalysisTask finished.");
    }

    private void createOrInitTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS public.tdm_road_popularity_test (
                way_id BIGINT PRIMARY KEY,
                frequence INT,
                popularity_tag VARCHAR(20)
            )
        """);
        jdbcTemplate.execute("TRUNCATE TABLE public.tdm_road_popularity_test");
    }

    private List<RoadStats> fetchWayFrequencies() {
        String sql = """
            SELECT way_id, COUNT(*) AS frequence
            FROM (
                SELECT UNNEST(road_list) AS way_id
                FROM public.dwd_trip_roads
            ) t
            GROUP BY way_id
        """;

        List<Map<String, Object>> result = jdbcTemplate.queryForList(sql);
        return result.stream()
                .map(row -> new RoadStats(
                        ((Number) row.get("way_id")).longValue(),
                        ((Number) row.get("frequence")).intValue()
                ))
                .collect(Collectors.toList());
    }

    private void assignPopularityTags(List<RoadStats> roads) {
        roads.sort(Comparator.comparingInt(RoadStats::getFrequence).reversed());

        List<Integer> freqs = roads.stream()
                .map(RoadStats::getFrequence)
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());

        int size = freqs.size();
        int q1 = freqs.get(size / 4);
        int q2 = freqs.get(size / 2);
        int q3 = freqs.get(size * 3 / 4);

        for (RoadStats r : roads) {
            int f = r.getFrequence();
            if (f >= q1) r.setPopularityTag("极热门路段");
            else if (f >= q2) r.setPopularityTag("热门路段");
            else if (f >= q3) r.setPopularityTag("普通路段");
            else r.setPopularityTag("冷门路段");
        }
    }

    private void batchInsert(List<RoadStats> roads) {
        String sql = """
            INSERT INTO public.tdm_road_popularity_test 
                (way_id, frequence, popularity_tag)
            VALUES (?, ?, ?)
        """;

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RoadStats r = roads.get(i);
                ps.setLong(1, r.getWayId());
                ps.setInt(2, r.getFrequence());
                ps.setString(3, r.getPopularityTag());
            }

            @Override
            public int getBatchSize() {
                return roads.size();
            }
        });
    }

    /** 内部类代表每条记录 */
    private static class RoadStats {
        private final long wayId;
        private final int frequence;
        private String popularityTag;

        public RoadStats(long wayId, int frequence) {
            this.wayId = wayId;
            this.frequence = frequence;
        }

        public long getWayId() { return wayId; }
        public int getFrequence() { return frequence; }
        public String getPopularityTag() { return popularityTag; }
        public void setPopularityTag(String popularityTag) { this.popularityTag = popularityTag; }
    }
}
