package com.mapfort.mapfortservice.controller;

import com.mapfort.mapfortservice.common.Result;
import com.mapfort.mapfortservice.entity.PickupRecommendationDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/recommend")
@RequiredArgsConstructor
@CrossOrigin
@Tag(name = "上客区推荐", description = "出租车司机上客区推荐相关接口")
public class PickupRecommendationController {

    private final JdbcTemplate jdbcTemplate;

    @GetMapping
    @Operation(
            summary = "获取Top5上客区域",
            description = "根据是否是工作日、当前时间获取Top5上客区域"
    )
    public Result<List<PickupRecommendationDTO>> getTopRecommendations(
            @RequestParam("workday") boolean workday,
            @RequestParam("hour") int hour
    ) {
        String sql = """
        SELECT way_name, district_name, pickup_count
        FROM public.ads_recommend_pickup_ways
        WHERE is_workday = ? AND stat_hour = ?
        ORDER BY pickup_count DESC
        LIMIT 5
    """;

        List<PickupRecommendationDTO> result = jdbcTemplate.query(sql, (rs, rowNum) -> {
            PickupRecommendationDTO dto = new PickupRecommendationDTO();
            dto.setWayName(rs.getString("way_name"));
            dto.setDistrictName(rs.getString("district_name"));
            dto.setPickupCount(rs.getInt("pickup_count"));
            return dto;
        }, workday, hour);

        return Result.success(result);
    }
}
