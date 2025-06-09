package com.pemex.pep.seemop.orchestrator.services;

import java.time.LocalDate;
import java.time.Year;
import java.time.format.DateTimeFormatter;

import io.quarkus.logging.Log;
import com.pemex.pep.seemop.orchestrator.models.EvaluacionEconomica;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Handles transition-related calculations and adjustments for economic
 * evaluations.
 * This class is responsible for managing partial year calculations and
 * adjusting
 * economic values proportionally based on the time period.
 */
@ApplicationScoped
public class TransitionHandler {

    /**
     * Calculates the fraction of a year represented by a given date.
     * 
     * @param date The date in format "dd/MM/yyyy"
     * @return A decimal value between 0 and 1 representing the fraction of the year
     */
    public double calculateYearFraction(String date) {
        try {
            LocalDate transitionDate = LocalDate.parse(date,
                    DateTimeFormatter.ofPattern("dd/MM/yyyy"));
            int daysInYear = Year.of(transitionDate.getYear()).length();
            int daysInPartialYear = transitionDate.getDayOfYear();
            return daysInPartialYear / (double) daysInYear;
        } catch (Exception e) {
            Log.error("Error calculating year fraction for date: " + date, e);
            return 1.0; // Default to full year if there's an error
        }
    }

    /**
     * Adjusts the values in an EvaluacionEconomica object for partial years.
     * This includes adjusting production rates and variable costs proportionally.
     * 
     * @param eval The economic evaluation to adjust
     * @param date The end date of the evaluation period in format "dd/MM/yyyy"
     */
    public void adjustPartialYearValues(EvaluacionEconomica eval, String date) {
        if (date == null || "unexist".equals(date)) {
            return;
        }

        double yearFraction = calculateYearFraction(date);

        // Adjust production
        if (eval.getProduccionDiariaPromedio() != null &&
                eval.getProduccionDiariaPromedio().getMbpce() != null) {
            double mbpce = eval.getProduccionDiariaPromedio().getMbpce();
            eval.getProduccionDiariaPromedio().setMbpce(mbpce * yearFraction);
        }

        // Adjust variable costs
        if (eval.getCostos() != null && eval.getCostos().getVariables() != null) {
            double variables = eval.getCostos().getVariables();
            eval.getCostos().setVariables(variables * yearFraction);
        }

        // Update total costs after adjusting variables
        if (eval.getCostos() != null) {
            double fijos = eval.getCostos().getFijos() != null ? eval.getCostos().getFijos() : 0.0;
            double variables = eval.getCostos().getVariables() != null ? eval.getCostos().getVariables() : 0.0;
            eval.getCostos().setTotal(fijos + variables);
        }
    }
}