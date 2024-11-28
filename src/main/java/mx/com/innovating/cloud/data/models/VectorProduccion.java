package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

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


}
