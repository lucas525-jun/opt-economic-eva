package com.pemex.pep.seemop.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeclinacionExponencialResult {
    private Integer vidSecuencia;
    private Integer vanio;
    private Integer vmes;
    private Integer vidOportunidadObjetivo;
    private Integer vidOportunidad;
    private String voportunidad;
    private String vclaveoportunidad;
    private String vclaveobjetivo;
    private Double vexpDeclinada;
    private Double vlnDeclinada;
    private Double vrecurso;
    private Integer vdiasmes;
    private Double vproduccion;
    private Double vcuota;
}