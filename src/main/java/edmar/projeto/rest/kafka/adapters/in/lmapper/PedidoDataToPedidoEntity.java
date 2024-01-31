package edmar.projeto.rest.kafka.adapters.in.lmapper;

import edmar.projeto.rest.kafka.adapters.in.entity.PedidoEntity;
import edmar.projeto.rest.kafka.data.PedidoData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Component
public class PedidoDataToPedidoEntity {

    @Autowired
    PedidoEntity pedidoEntity;


    public PedidoEntity toPedidoEntity(PedidoData pedidoData){
        pedidoEntity.setId(pedidoData.getId());
        pedidoEntity.setPago(pedidoData.isPago());
        pedidoEntity.setValor(pedidoData.getValor());
        pedidoEntity.setProduto(pedidoData.getProduto());
        pedidoEntity.setCliente(pedidoData.getCliente());
        //pedidoEntity.setSeparado(pedidoData.isSeparado());
        return pedidoEntity;
    }

}
