package com.pemex.pep.seemop.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrecioHidrocarburo {

	private Integer idhidrocarburo;
	private Long idoportunidadobjetivo;
	private String hidrocarburo;
	private String fecha;
	private Double precio;

}
