package edmar.projeto.rest.kafka.adapters.in.repository;

import edmar.projeto.rest.kafka.adapters.in.entity.PedidoEntity;
import org.springframework.data.mongodb.repository.MongoRepository;


public interface PedidoRepository extends MongoRepository<PedidoEntity, String> {
}
