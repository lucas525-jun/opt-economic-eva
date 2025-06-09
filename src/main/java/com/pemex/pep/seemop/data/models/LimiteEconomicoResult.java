package com.pemex.pep.seemop.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LimiteEconomicoResult {
    private Integer vidOportunidadObjetivo;
    private Integer vidOportunidad;
    private String voportunidad;
    private Double vpce;
    private Integer vlimiteEconomico;
    private Integer vmeseslecon;
}