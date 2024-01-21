package com.example.springbatchdemo;

import lombok.*;

import java.math.BigDecimal;

/**
 * @author Sergei Aleshchenko
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SalesReportItem {
    private Long regionId;
    private Long outletId;
    private BigDecimal smartphones;
    private BigDecimal memoryCards;
    private BigDecimal notebooks;
    private BigDecimal total;
}
