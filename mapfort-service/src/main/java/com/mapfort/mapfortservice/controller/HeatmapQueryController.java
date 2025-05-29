package com.mapfort.mapfortservice.controller;

import com.mapfort.mapfortservice.entity.HeatMapPointsDTO;
import com.mapfort.mapfortservice.utils.CoordinateTransformUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import com.mapfort.mapfortservice.common.Result;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mapfort.mapfortservice.common.Result.success;

@RestController
@RequestMapping("/api/heatmap")
@RequiredArgsConstructor
@CrossOrigin
@Tag(name = "热力图渲染", description = "热力图渲染相关接口")
public class HeatmapQueryController {

    private final JdbcTemplate jdbcTemplate;

    /**
     * 查询指定星期几和小时的热力图点数据（用于周期性热力图）
     *
     * @param workday   星期几，0 = 周日，1 = 周一，... 6 = 周六
     * @param hour  小时（0~23）
     */
    @GetMapping
    @Operation(
            summary = "获取区域热度List",
            description = "根据是否是工作日、当前时间获取区域热度List"
    )
    public Result<List<HeatMapPointsDTO>> getHeatmapByDayAndHour(
            @RequestParam("workday") boolean workday,
            @RequestParam("hour") int hour
    ) {
        String sql = """
        SELECT lon_center AS lon, lat_center AS lat, pickup_count AS intensity
        FROM public.tdm_hot_grids
        WHERE is_workday = ? AND stat_hour = ?
    """;

        List<HeatMapPointsDTO> result = jdbcTemplate.query(sql, (rs, rowNum) -> {
            double lng = rs.getDouble("lon");
            double lat = rs.getDouble("lat");
            int intensity = rs.getInt("intensity");
            double[] gcj = CoordinateTransformUtil.wgs2gcj(lng, lat);

            HeatMapPointsDTO dto = new HeatMapPointsDTO();
            dto.setLon(gcj[0]);
            dto.setLat(gcj[1]);
            dto.setIntensity(intensity);
            return dto;
        }, workday, hour);

        return Result.success(result);
    }
}
