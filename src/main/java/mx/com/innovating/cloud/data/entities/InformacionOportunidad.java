package mx.com.innovating.cloud.data.entities;

import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.Subselect;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@Entity
@Immutable
@Subselect("select * from catalogo.claveobjetivovw")
@Table(name = "claveobjetivovw", schema = "catalogo")
@EqualsAndHashCode(callSuper = false)
public class InformacionOportunidad extends PanacheEntityBase {

	@Id
	private Integer idproyecto;
	private String proyecto;
	private Integer idoportunidadobjetivo;
	private Integer idoportunidad;
	private String oportunidad;
	private Integer idobjetivo;
	private String objetivo;
	private String fechainicioperfexploratorio;
	private String fechainicio;
	private String claveoportunidad;
	private String claveobjetivo;
	private Double duracionperfpozodesarrollo;
	private Double duraciontermpozodesarrollo;
	private Integer idhidrocarburo;
	private String hidrocarburo;
	private Integer idtipooportunidad;
	private String tipooportunidad;
	private Integer idprograma;
	private String programa;
	private Integer idversion;

	public static InformacionOportunidad findByIdoportunidadobjetivo(Integer idOportunidad) {
		return find("idoportunidadobjetivo", idOportunidad).firstResult();
	}

}
