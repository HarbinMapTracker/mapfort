package com.mapfort.mapfortservice.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
@Schema(description = "接客推荐信息")
public class PickupRecommendationDTO {
    @Schema(description = "道路名", example = "江杨北路")
    private String wayName;
    @Schema(description = "行政区名", example = "南岗区")
    private String districtName;
    @Schema(description = "接客数量", example = "150")
    private Integer pickupCount;
}
