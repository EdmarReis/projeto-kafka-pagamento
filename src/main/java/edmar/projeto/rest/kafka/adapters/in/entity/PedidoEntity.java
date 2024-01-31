package edmar.projeto.rest.kafka.adapters.in.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Document(collection = "pedidoPagamento")
@Data
@Component
public class PedidoEntity {

    @Id
    private String id;
    private String cliente;
    private String produto;
    private BigDecimal valor;
    private boolean pago;
    //private boolean separado;

}
