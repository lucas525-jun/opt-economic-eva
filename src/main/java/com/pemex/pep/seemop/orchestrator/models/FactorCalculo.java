package com.pemex.pep.seemop.orchestrator.models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class FactorCalculo {

    @Id // Solo si estás mapeando con JPA y tienes una clave primaria
    private Long idoportunidadobjetivo;
    private Integer idversion;
    private Double fc_aceite;
    private Double fc_gas;
    private Double fc_condensado;

    public FactorCalculo() {
    }

    // Getters y Setters
    public Long getIdoportunidadobjetivo() {
        return idoportunidadobjetivo;
    }

    public void setIdoportunidadobjetivo(Long idoportunidadobjetivo) {
        this.idoportunidadobjetivo = idoportunidadobjetivo;
    }

    public Integer getIdversion() {
        return idversion;
    }

    public void setIdversion(Integer idversion) {
        this.idversion = idversion;
    }

    public Double getFc_aceite() {
        return fc_aceite;
    }

    public void setFc_aceite(Double fcAceite) {
        this.fc_aceite = fcAceite;
    }

    public Double getFc_gas() {
        return fc_gas;
    }

    public void setFc_gas(Double fcGas) {
        this.fc_gas = fcGas;
    }

    public Double getFc_condensado() {
        return fc_condensado;
    }

    public void setFc_condensado(Double fcCondensado) {
        this.fc_condensado = fcCondensado;
    }

    public FactorCalculo(Long idoportunidadobjetivo, Integer idversion, Double fc_aceite, Double fc_gas,
            Double fc_condensado) {
        this.idoportunidadobjetivo = idoportunidadobjetivo;
        this.idversion = idversion;
        this.fc_aceite = fc_aceite;
        this.fc_gas = fc_gas;
        this.fc_condensado = fc_condensado;
    }
}
