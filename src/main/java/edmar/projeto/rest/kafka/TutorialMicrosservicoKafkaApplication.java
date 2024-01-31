package edmar.projeto.rest.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
@ComponentScan("edmar.projeto.rest.kafka")
public class TutorialMicrosservicoKafkaApplication {

	public static void main(String[] args) {

		SpringApplication.run(TutorialMicrosservicoKafkaApplication.class, args);

	}

}
