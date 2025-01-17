package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import mx.com.innovating.cloud.data.models.DeclinaExpoportunidadResult;
import mx.com.innovating.cloud.data.models.DeclinadaMes;
import mx.com.innovating.cloud.data.models.LimiteEconomicoResult;

import mx.com.innovating.cloud.data.models.DeclinadaExp;
import mx.com.innovating.cloud.data.calculator.LimiteEconomicoService;

import java.util.ArrayList;
import java.util.List;
import java.util.Calendar;
import java.util.Date;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.stream.Collectors;

@ApplicationScoped
public class DeclinaExpOportunidadService {

        @Inject
        EntityManager em;

        @Inject
        LimiteEconomicoService limiteEconomicoService;

        @Transactional
        @CacheResult(cacheName = "oportunidad-base-data")
        protected List<Object[]> getBaseData(
                        @CacheKey int pnidversion,
                        @CacheKey int pnoportunidadobjetivo) {

                String query = """
                                    SELECT
                                        de.idOportunidadObjetivo,
                                        de.idOportunidad,
                                        de.oportunidad,
                                        de.claveOportunidad,
                                        de.claveObjetivo,
                                        de.fechaInicio,
                                        SUM(de.duracionPerfPozoDesarrollo + de.duracionTermPozoDesarrollo),
                                        de.idVersion,
                                        de.hidrocarburo
                                    FROM catalogo.claveobjetivovw de
                                    WHERE de.idOportunidadObjetivo = :pnoportunidadobjetivo
                                    AND de.idVersion = :pnidversion
                                    GROUP BY
                                        de.idOportunidadObjetivo,
                                        de.idOportunidad,
                                        de.oportunidad,
                                        de.claveOportunidad,
                                        de.claveObjetivo,
                                        de.fechaInicio,
                                        de.idVersion,
                                        de.hidrocarburo
                                """;

                return em.createNativeQuery(query)
                                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                                .setParameter("pnidversion", pnidversion)
                                .getResultList();
        }

        DeclinadaMes createDeclinadaMes(Object[] baseData, double pndeclinada, double pncuota, double pnpce,
                        int meses) {
                DeclinadaMes declinada = new DeclinadaMes();
                declinada.setIdOportunidadObjetivo((Integer) baseData[0]);
                declinada.setIdOportunidad((Integer) baseData[1]);
                declinada.setOportunidad((String) baseData[2]);
                declinada.setClaveOportunidad((String) baseData[3]);
                declinada.setClaveObjetivo((String) baseData[4]);
                declinada.setFecha("01/01/" + baseData[5].toString());
                declinada.setDias(((Number) baseData[6]).intValue());
                declinada.setIdVersion((Integer) baseData[7]);
                declinada.setMeses(meses);
                declinada.setLnDeclinada(-Math.log(1 - pndeclinada / 100) / 12);
                declinada.setPrimDeclinacionOportunidad(pndeclinada);
                declinada.setGastoInicial(pncuota);
                declinada.setRecurso(pnpce);

                // Calculate mesInicio
                Calendar cal = Calendar.getInstance();
                cal.set(Calendar.YEAR, Integer.parseInt(baseData[5].toString()));
                cal.set(Calendar.MONTH, Calendar.JANUARY);
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.add(Calendar.DAY_OF_YEAR, declinada.getDias());
                declinada.setMesInicio(cal.getTime());

                return declinada;
        }

        public List<DeclinadaExp> calculateDeclinadaExp(DeclinadaMes baseDeclina) {
                List<DeclinadaExp> declinadaExpList = new ArrayList<>();
                Calendar cal = Calendar.getInstance();
                cal.setTime(baseDeclina.getMesInicio());

                // Generate monthly series
                for (int i = 1; i <= baseDeclina.getMeses(); i++) {
                        DeclinadaExp exp = new DeclinadaExp();
                        exp.setIdTempExp(i);
                        exp.setIdOportunidadObjetivo(baseDeclina.getIdOportunidadObjetivo());
                        exp.setFecha(cal.getTime());
                        exp.setNMes(cal.get(Calendar.MONTH) + 1);
                        exp.setAnio(cal.get(Calendar.YEAR));

                        // Calculate days in month
                        cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH));
                        exp.setDiasMes(cal.get(Calendar.DAY_OF_MONTH));

                        declinadaExpList.add(exp);
                        cal.add(Calendar.MONTH, 1);
                }

                // Calculate exponential decline
                double gastoActual = baseDeclina.getGastoInicial();
                double lnDeclinada = baseDeclina.getLnDeclinada();

                for (DeclinadaExp exp : declinadaExpList) {
                        if (exp.getIdTempExp() == 1) {
                                exp.setExpDeclinada(baseDeclina.getGastoInicial());
                        } else {
                                exp.setExpDeclinada(gastoActual * Math.exp(-1 * lnDeclinada));
                        }
                        gastoActual = exp.getExpDeclinada();
                }

                return declinadaExpList;
        }

        @Transactional
        public List<DeclinaExpoportunidadResult> calcularDeclinaExpoportunidad(
                        int pnidversion,
                        int pnoportunidadobjetivo,
                        double pncuota,
                        double pndeclinada,
                        double pnpce) {

                // Get cached base data
                List<Object[]> baseData = getBaseData(pnidversion, pnoportunidadobjetivo);

                if (baseData.isEmpty()) {
                        return new ArrayList<>();
                }

                // Get meses from limite economico calculation (not cached)
                List<LimiteEconomicoResult> limiteEconomico = limiteEconomicoService
                                .calcularLimiteEconomico(pnidversion, pnoportunidadobjetivo, pnpce);

                int meses = limiteEconomico.get(0).getVmeseslecon();

                // Create DeclinadaMes from base data
                DeclinadaMes baseDeclina = createDeclinadaMes(
                                baseData.get(0), pndeclinada, pncuota, pnpce, meses);

                // Calculate DeclinadaExp series
                List<DeclinadaExp> declinadaExpList = calculateDeclinadaExp(baseDeclina);

                // Map to final results
                return declinadaExpList.stream()
                                .map(exp -> new DeclinaExpoportunidadResult(
                                                exp.getIdTempExp(),
                                                exp.getAnio(),
                                                exp.getNMes(),
                                                exp.getIdOportunidadObjetivo(),
                                                baseDeclina.getIdOportunidad(),
                                                baseDeclina.getOportunidad(),
                                                baseDeclina.getClaveOportunidad(),
                                                baseDeclina.getClaveObjetivo(),
                                                exp.getExpDeclinada(),
                                                baseDeclina.getLnDeclinada(),
                                                baseDeclina.getRecurso(),
                                                exp.getDiasMes(),
                                                (exp.getExpDeclinada() * exp.getDiasMes()) / 1000,
                                                baseDeclina.getGastoInicial()))
                                .toList();
        }
}