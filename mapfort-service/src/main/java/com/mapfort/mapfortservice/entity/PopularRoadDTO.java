package com.mapfort.mapfortservice.entity;

import lombok.Data;

@Data
public class PopularRoadDTO {
    private String roadName;        // 路名
    private int frequence;          // 访问频率
    private String popularityTag;   // 热度标签
}
