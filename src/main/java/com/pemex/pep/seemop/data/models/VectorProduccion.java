package com.pemex.pep.seemop.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VectorProduccion {

	private String voportunidad;
	private String vobjetivo;
	private Integer vidoportunidadobjetivo;
	private String aanio;
	private double ctotalmes;
	private double ctotalanual;
	private double ctotalaceite;
	private double ctotalgas;
	private double ctotalcondensado;

}
