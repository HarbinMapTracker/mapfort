package com.mapfort.mapfortservice.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
@Schema(description = "热力图渲染信息")
public class HeatMapPointsDTO {
    @Schema(description = "热度", example = "2")
    private Integer intensity;
    @Schema(description = "纬度", example = "45.75847358072181")
    private double lat;
    @Schema(description = "经度", example = "126.67755255411943")
    private double lon;
}
