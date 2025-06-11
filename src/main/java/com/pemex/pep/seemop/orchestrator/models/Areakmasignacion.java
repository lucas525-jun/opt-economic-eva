package com.pemex.pep.seemop.orchestrator.models;


import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Areakmasignacion {

    @Id
    private Integer idoportunidadobjetivo;

    private Integer idoportunidad;
    private Integer idversion;
    private Double areakmasignacion;
    private Double pg;
}
