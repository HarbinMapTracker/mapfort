package com.mapfort.datacompute.adsUtils;

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
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@EnableScheduling
@RequiredArgsConstructor
public class RoadPopularityStatisticsTask {

    private final JdbcTemplate jdbcTemplate;

    // 定时任务，定期执行统计
//    @Scheduled(cron = "0 0 3 * * ?")  // 每天凌晨 3 点执行
    @PostConstruct
    @Transactional
    public void run() {
        log.info("DWD -> ADS: RoadPopularityStatisticsTask started.");
        createOrInitTable();  // 创建或初始化表
        List<RoadStats> roads = fetchWayFrequencies();  // 获取路段访问频率
        assignPopularityTags(roads);  // 分配热度标签
        batchInsert(roads);  // 批量插入数据
        log.info("DWD -> ADS: RoadPopularityStatisticsTask finished.");
    }

    // 创建新的ads_road_popularity_statistics表
    private void createOrInitTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS public.ads_road_popularity_statistics (
                road_name VARCHAR(255),  -- 路名
                frequence INT,           -- 访问频率
                popularity_tag VARCHAR(20) -- 热度标签
            )
        """);
        jdbcTemplate.execute("TRUNCATE TABLE public.ads_road_popularity_statistics");  // 清空表数据
    }

    // 从dwd_trip_roads获取路段访问频率
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

    // 根据访问频率为路段分配热度标签
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

    // 批量插入数据到ads_road_popularity_statistics表中
    private void batchInsert(List<RoadStats> roads) {
        // 获取路段名称
        String getRoadNamesSql = """
        SELECT gid, tags::jsonb ->> 'name' AS road_name
        FROM public.dwd_roads
        WHERE tags::jsonb ->> 'name' IS NOT NULL  -- 过滤掉没有name的记录
    """;

        Map<Long, String> roadNameMap = new HashMap<>();
        List<Map<String, Object>> roadNames = jdbcTemplate.queryForList(getRoadNamesSql);
        for (Map<String, Object> row : roadNames) {
            roadNameMap.put(((Number) row.get("gid")).longValue(), (String) row.get("road_name"));
        }

        // 过滤掉没有有效路名的RoadStats
        List<RoadStats> validRoads = roads.stream()
                .filter(r -> roadNameMap.containsKey(r.getWayId()) && roadNameMap.get(r.getWayId()) != null)
                .collect(Collectors.toList());

        if (validRoads.isEmpty()) {
            log.info("No valid road names to insert.");
            return;
        }

        String sql = """
        INSERT INTO public.ads_road_popularity_statistics 
            (road_name, frequence, popularity_tag)
        VALUES (?, ?, ?)
    """;

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RoadStats r = validRoads.get(i);
                // 根据way_id查找对应的road_name
                String roadName = roadNameMap.getOrDefault(r.getWayId(), "未知路段");
                ps.setString(1, roadName);  // 设置路名
                ps.setInt(2, r.getFrequence());  // 设置频率
                ps.setString(3, r.getPopularityTag());  // 设置热度标签
            }

            @Override
            public int getBatchSize() {
                return validRoads.size();
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
