package com.pemex.pep.seemop.data.calculator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import com.pemex.pep.seemop.data.models.ProduccionPozos;
import com.pemex.pep.seemop.data.models.DeclinaExpoportunidadResult;

import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class PozoVolumenService {

    @Inject
    EntityManager em;

    @Inject
    DeclinaExpOportunidadService declinaExpOportunidadService;

    public List<ProduccionPozos> calcularPozoVolumen(
            int pnidversion,
            int pnoportunidadobjetivo,
            double pncuota,
            double pndeclinada,
            double pnpce) {

        List<DeclinaExpoportunidadResult> declinaResults = declinaExpOportunidadService
                .calcularDeclinaExpoportunidad(pnidversion, pnoportunidadobjetivo, pncuota, pndeclinada, pnpce);

        // First, group the results
        Map<GroupKey, Double> groupedResults = declinaResults.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(
                        this::createGroupKey,
                        Collectors.summingDouble(DeclinaExpoportunidadResult::getVexpDeclinada)));

        // Then transform to final results
        return groupedResults.entrySet().stream()
                .map(entry -> createPozoVolumenResult(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private GroupKey createGroupKey(DeclinaExpoportunidadResult result) {
        return new GroupKey(
                result.getVidOportunidadObjetivo(),
                result.getVidOportunidad(),
                result.getVoportunidad(),
                result.getVclaveoportunidad(),
                result.getVclaveobjetivo(),
                result.getVrecurso());
    }

    private ProduccionPozos createPozoVolumenResult(GroupKey key, Double totalExpDeclinada) {
        double prodAcumulada = calculateProdAcumulada(totalExpDeclinada);
        double numPozo = calculateNumPozo(key.getRecurso(), totalExpDeclinada);

        return new ProduccionPozos(
                key.getIdOportunidadObjetivo(),
                key.getIdOportunidad(),
                key.getOportunidad(),
                key.getClaveOportunidad(),
                key.getClaveObjetivo(),
                prodAcumulada,
                numPozo);
    }

    private double calculateProdAcumulada(double totalExpDeclinada) {
        return (totalExpDeclinada * 30.417) / 1000;
    }

    private double calculateNumPozo(Double recurso, double totalExpDeclinada) {
        if (totalExpDeclinada == 0) {
            return 0;
        }
        double prodAcumulada = calculateProdAcumulada(totalExpDeclinada);
        return recurso / prodAcumulada;
    }

    // Improved GroupKey class with proper encapsulation
    private static class GroupKey {
        private final Integer idOportunidadObjetivo;
        private final Integer idOportunidad;
        private final String oportunidad;
        private final String claveOportunidad;
        private final String claveObjetivo;
        private final Double recurso;

        public GroupKey(Integer idOportunidadObjetivo, Integer idOportunidad, String oportunidad,
                String claveOportunidad, String claveObjetivo, Double recurso) {
            this.idOportunidadObjetivo = Objects.requireNonNull(idOportunidadObjetivo,
                    "idOportunidadObjetivo cannot be null");
            this.idOportunidad = Objects.requireNonNull(idOportunidad, "idOportunidad cannot be null");
            this.oportunidad = Objects.requireNonNull(oportunidad, "oportunidad cannot be null");
            this.claveOportunidad = Objects.requireNonNull(claveOportunidad, "claveOportunidad cannot be null");
            this.claveObjetivo = Objects.requireNonNull(claveObjetivo, "claveObjetivo cannot be null");
            this.recurso = Objects.requireNonNull(recurso, "recurso cannot be null");
        }

        // Getters
        public Integer getIdOportunidadObjetivo() {
            return idOportunidadObjetivo;
        }

        public Integer getIdOportunidad() {
            return idOportunidad;
        }

        public String getOportunidad() {
            return oportunidad;
        }

        public String getClaveOportunidad() {
            return claveOportunidad;
        }

        public String getClaveObjetivo() {
            return claveObjetivo;
        }

        public Double getRecurso() {
            return recurso;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            GroupKey groupKey = (GroupKey) o;
            return Objects.equals(idOportunidadObjetivo, groupKey.idOportunidadObjetivo) &&
                    Objects.equals(idOportunidad, groupKey.idOportunidad) &&
                    Objects.equals(oportunidad, groupKey.oportunidad) &&
                    Objects.equals(claveOportunidad, groupKey.claveOportunidad) &&
                    Objects.equals(claveObjetivo, groupKey.claveObjetivo) &&
                    Objects.equals(recurso, groupKey.recurso);
        }

        @Override
        public int hashCode() {
            return Objects.hash(idOportunidadObjetivo, idOportunidad, oportunidad,
                    claveOportunidad, claveObjetivo, recurso);
        }
    }
}