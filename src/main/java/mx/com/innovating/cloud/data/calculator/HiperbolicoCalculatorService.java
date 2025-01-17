package mx.com.innovating.cloud.data.calculator;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
public class HiperbolicoCalculatorService {

    public List<HiperbolicoOportunidadResult> calculateMonthlyResults(
            DatosHiperbolicosInput datos,
            int pnmeses,
            LocalDate startDate) {

        List<HiperbolicoOportunidadResult> results = new ArrayList<>();

        double w_hiperbolico = datos.getHiperbolico();
        double w_hiperbolicoajustada = datos.getGastoInicial();
        double w_hiperbolicoant = datos.getHiperbolico();
        double w_hiperbolicoajustadaant = w_hiperbolicoajustada;
        double w_b = datos.getB();
        double w_dim = datos.getDimen();
        double w_di = datos.getDi();
        double w_gasto = datos.getGastoInicial();
        double w_rga = datos.getRga();
        double w_frcmedio = datos.getFrcMedio();

        for (int w_contador = 1; w_contador <= pnmeses; w_contador++) {
            double w_porcentaje = 0.0;
            double w_porcentajeajustada = 0.0;

            LocalDate currentDate = startDate.plusMonths(w_contador - 1);
            YearMonth yearMonth = YearMonth.from(currentDate);

            if (w_contador == 1) {
                w_hiperbolico = w_gasto;
                w_hiperbolicoajustada = w_hiperbolicoajustadaant;
                w_hiperbolicoajustadaant = w_gasto;
            } else {
                w_hiperbolico = w_gasto / Math.pow(1 + w_b * w_di * (w_contador - 1), 1 / w_b);
                w_porcentaje = 1 - (w_hiperbolico / w_hiperbolicoant);
                w_porcentajeajustada = w_porcentaje > w_dim ? w_porcentaje : w_dim;
                w_hiperbolicoajustada = w_hiperbolicoajustadaant - (w_hiperbolicoajustadaant * w_porcentajeajustada);
                w_hiperbolicoajustadaant = w_hiperbolicoajustada;
            }

            w_hiperbolicoant = w_hiperbolico;

            HidrocarburosCalculo hidrocarburos = calculateHidrocarburos(
                    datos.getHidrocarburo(),
                    w_hiperbolicoajustada,
                    w_hiperbolico,
                    w_rga,
                    w_frcmedio);

            double produccion = (w_hiperbolicoajustada * yearMonth.lengthOfMonth()) / 1000;
            double pmbpce = hidrocarburos.aceite() + hidrocarburos.condensado() + (hidrocarburos.gas() / 5);

            results.add(createResult(
                    w_contador,
                    currentDate,
                    datos,
                    w_hiperbolico,
                    w_porcentaje,
                    w_porcentajeajustada,
                    w_hiperbolicoajustada,
                    yearMonth.lengthOfMonth(),
                    produccion,
                    w_gasto,
                    hidrocarburos,
                    pmbpce));
        }

        return results;
    }

    private record HidrocarburosCalculo(double aceite, double gas, double condensado) {
    }

    private HidrocarburosCalculo calculateHidrocarburos(
            String hidrocarburo,
            double hiperbolicoAjustada,
            double hiperbolico,
            double rga,
            double frcmedio) {

        double aceite = 0.0, gas = 0.0, condensado = 0.0;

        if (hidrocarburo.startsWith("Aceite")) {
            aceite = hiperbolicoAjustada;
            gas = hiperbolicoAjustada * rga;
            condensado = (gas * frcmedio) / 1000;
        } else if (hidrocarburo.startsWith("Gas")) {
            gas = hiperbolicoAjustada;
            condensado = (hiperbolico * frcmedio) / 1000;
        } else if (hidrocarburo.startsWith("Condensado")) {
            gas = (1000 * hiperbolicoAjustada) / frcmedio;
            condensado = hiperbolicoAjustada;
        }

        return new HidrocarburosCalculo(aceite, gas, condensado);
    }

    private HiperbolicoOportunidadResult createResult(
            int sequence,
            LocalDate date,
            DatosHiperbolicosInput datos,
            double hiperbolico,
            double porcentaje,
            double porcentajeAjustada,
            double hiperbolicoAjustada,
            int diasMes,
            double produccion,
            double gasto,
            HidrocarburosCalculo hidrocarburos,
            double pmbpce) {

        return new HiperbolicoOportunidadResult(
                sequence,
                date.getYear(),
                date.getMonthValue(),
                datos.getIdTipoHidrocarburo(),
                datos.getHidrocarburo(),
                datos.getIdoportunidadobjetivo(),
                datos.getIdoportunidad(),
                datos.getOportunidad(),
                datos.getClaveoportunidad(),
                datos.getClaveobjetivo(),
                hiperbolico,
                porcentaje,
                porcentajeAjustada,
                hiperbolicoAjustada,
                diasMes,
                produccion,
                gasto,
                hidrocarburos.aceite(),
                hidrocarburos.gas(),
                hidrocarburos.condensado(),
                pmbpce);
    }
}