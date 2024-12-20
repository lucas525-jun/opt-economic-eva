package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Inversiones {

	private Double total;

	private Double exploratoria;
	private Double perforacionExp;
	private Double terminacionExp;
	private Double infraestructuraExp;

	private Double desarrolloSinOperacional;

	private Double perforacionDes;
	private Double terminacionDes;
	private Double infraestructuraDes;
	private Double lineaDescarga;
	private Double ductos;
	private Double plataformaDesarrollo;
	private Double sistemaDeControl;
	private Double risers;
	private Double manifolds;
	private Double arbolSubmarinos;
	private Double estacionCompresion;
	private Double bateria;
	private Double cubiertaProcesos;
	private Double buqueTanqueCompra;
	private Double buqueTanqueRenta;

	private Double operacionalFuturoDesarrollo;

	private Double desarrollo;

}
