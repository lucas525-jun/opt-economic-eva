package mx.com.innovating.cloud.orchestrator.models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class FactorCalculo {

    @Id // Solo si estás mapeando con JPA y tienes una clave primaria
    private Integer idoportunidadobjetivo;
    private Integer idversion;
    private Double fc_aceite;
    private Double fc_gas;
    private Double fc_condensado;

    // Getters y Setters
    public Integer getIdoportunidadobjetivo() {
        return idoportunidadobjetivo;
    }

    public void setIdoportunidadobjetivo(Integer idoportunidadobjetivo) {
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
}
