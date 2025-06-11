package com.pemex.pep.seemop.data.calculator;

import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

import com.pemex.pep.seemop.data.models.NumEquipoResult;

@ApplicationScoped
public class NumEquipoService {

    @Transactional
    @CacheResult(cacheName = "num-equipo-cache")
    public NumEquipoResult calcularNumEquipo(@CacheKey Integer numPozo) {
        int numEquipo;

        if (numPozo > 0 && numPozo < 20) {
            numEquipo = 2;
        } else if (numPozo >= 20 && numPozo < 40) {
            numEquipo = 4;
        } else if (numPozo >= 40 && numPozo < 100) {
            numEquipo = 8;
        } else {
            numEquipo = 16;
        }

        return new NumEquipoResult(numEquipo, numPozo);
    }
}