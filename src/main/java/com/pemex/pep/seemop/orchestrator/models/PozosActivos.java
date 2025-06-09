package com.pemex.pep.seemop.orchestrator.models;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PozosActivos {

	private String anio;
	private BigDecimal promedioAnual;

}
