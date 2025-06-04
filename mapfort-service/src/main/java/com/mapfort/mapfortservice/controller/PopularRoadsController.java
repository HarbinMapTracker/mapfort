package com.mapfort.mapfortservice.controller;

import com.mapfort.mapfortservice.common.Result;
import com.mapfort.mapfortservice.entity.PopularRoadDTO;  // 你需要创建这个 DTO 类
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import jakarta.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/api/popular-roads")
@RequiredArgsConstructor
@CrossOrigin
@Tag(name = "热门路段", description = "根据访问频率和热度标签获取热门路段相关接口")
public class PopularRoadsController {

    private final JdbcTemplate jdbcTemplate;

    @GetMapping
    @Operation(
            summary = "获取热门路段",
            description = "根据热度标签和访问频率获取Top热门路段"
    )
    public Result<List<PopularRoadDTO>> getTopPopularRoads(
            @RequestParam(value = "limit", defaultValue = "50") int limit  // 获取Top N条热门路段，默认为50条
    ) {
        String sql = """
        SELECT road_name, frequence, popularity_tag
        FROM public.ads_road_popularity_statistics
        ORDER BY frequence DESC
        LIMIT ?
        """;

        List<PopularRoadDTO> result = jdbcTemplate.query(sql, (rs, rowNum) -> {
            PopularRoadDTO dto = new PopularRoadDTO();
            dto.setRoadName(rs.getString("road_name"));
            dto.setFrequence(rs.getInt("frequence"));
            dto.setPopularityTag(rs.getString("popularity_tag"));
            return dto;
        }, limit);

        return Result.success(result);
    }

    @GetMapping("/report")
    @Operation(
            summary = "获取热门路段csv表格",
            description = "根据热度标签和访问频率获取Top热门路段"
    )
    public void downloadReport(@RequestParam(defaultValue = "50") int limit, HttpServletResponse response) throws IOException {
        // 查询前50热门路段数据
        String sql = """
        SELECT road_name, frequence, popularity_tag
        FROM public.ads_road_popularity_statistics
        ORDER BY frequence DESC
        LIMIT ? 
        """;

        List<PopularRoadDTO> roads = jdbcTemplate.query(sql, (rs, rowNum) -> {
            PopularRoadDTO dto = new PopularRoadDTO();
            dto.setRoadName(rs.getString("road_name"));
            dto.setFrequence(rs.getInt("frequence"));
            dto.setPopularityTag(rs.getString("popularity_tag"));
            return dto;
        }, limit);

        // 设置响应头，标识文件类型为CSV并且为附件下载
        response.setContentType("text/csv");
        response.setHeader("Content-Disposition", "attachment; filename=\"热门路段报表.csv\"");

        // 设置字符编码为 UTF-8
        response.setCharacterEncoding("UTF-8");

        // 输出 CSV 文件，确保是 UTF-8 编码
        OutputStream outputStream = response.getOutputStream();

        // 加上 BOM 头，确保 Excel 识别 UTF-8 编码
        outputStream.write(0xEF);
        outputStream.write(0xBB);
        outputStream.write(0xBF);  // BOM 字节顺序标记

        // 使用 OutputStreamWriter 确保是 UTF-8 编码
        Writer writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        writer.write("路名,访问频率,热度标签\n");

        // 写入数据
        for (PopularRoadDTO road : roads) {
            writer.write(String.format("%s,%d,%s\n", road.getRoadName(), road.getFrequence(), road.getPopularityTag()));
        }

        writer.flush();
        outputStream.flush();
    }
}
