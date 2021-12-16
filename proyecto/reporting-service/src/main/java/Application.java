/*
 *  MIT License
 *
 *  Copyright (c) 2019 Michael Pogrebinsky - Distributed Systems & Cloud Computing with Java
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Application {
    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        String consumerGroup = "reporting-service-group";

        System.out.println("Consumidores es parte del grupo " + consumerGroup);

        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        consumeMessages(Collections.unmodifiableList(Arrays.asList(SUSPICIOUS_TRANSACTIONS_TOPIC, VALID_TRANSACTIONS_TOPIC)), kafkaConsumer);
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        /**
         * Complete el código aquí para suscribirse a los temas proporcionados
         * Ejecute en un loop un mecanismos que consuma todas las transacciones
         * Registre las transacciones para informar según el tema(topic)
         */

        kafkaConsumer.subscribe(topics);


        while (true) {
            //Se obtiene una lista con los nuevos mensajes del topic asociado al consumer
            ConsumerRecords<String, Transaction> cRecord = kafkaConsumer.poll(Duration.ofSeconds(1));

            //Se verifica que efectivamente exista algun mensaje nuevo
            if(!cRecord.isEmpty()){
                for(ConsumerRecord<String,Transaction> cr: cRecord){
                    //Se envia la notificacion
                    recordTransactionForReporting(cr.topic(),cr.value());
                }
            }
            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        /**
         * Configure todos los parámetros del cliente de Kafka aquí
         * Cree y devuelva un nuevo consumidor de Kafka
         */

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers/** Complete el codigo aca en caso que sea necesario **/);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() /** Complete el codigo aca en caso que sea necesario **/);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName()/** Complete el codigo aca en caso que sea necesario **/);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,consumerGroup /** Complete el codigo aca en caso que sea necesario **/);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(properties);
    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        if (topic.equals(SUSPICIOUS_TRANSACTIONS_TOPIC)) {
            System.out.println(String.format("Registro de transacciones sospechosas para el usuario %s, por la cantidad de " +
                            "$%.2f originario de %s para futuras investigaciones",
                    transaction.getUser(), transaction.getAmount(), transaction.getTransactionLocation()));

        } else if (topic.equals(VALID_TRANSACTIONS_TOPIC)) {
            System.out.println(String.format("Registro de transacciones para el usuario %s, por la cantidad de $%.2f para notificar a los usuarios " +
                    "su estado mensual",
                    transaction.getUser(), transaction.getAmount()));
        }
    }

}
