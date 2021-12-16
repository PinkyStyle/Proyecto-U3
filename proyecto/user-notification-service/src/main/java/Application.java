
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        String consumerGroup = "user-notification-service-group";

        System.out.println("Consumidores es parte del grupo " + consumerGroup);

        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        consumeMessages(SUSPICIOUS_TRANSACTIONS_TOPIC, kafkaConsumer);
    }

    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
        /**
         * Complete el código aquí para suscribirse al tema(topic) correcto.*/
          kafkaConsumer.subscribe(Collections.singleton(topic));
         /**
         * Ejecute un loop y lea las transacciones entrantes
         * Para cada nueva transacción, envíe una notificación al usuario
         */

        while (true) {
            /**
             * Complete el codigo aca en caso que sea necesario
             * Verifique si hay nuevas transacciones a leer desde Kafka.
             * Apruebe las transacciones entrantes
             */

            //Se obtiene una lista con los nuevos mensajes del topic asociado al consumer
            ConsumerRecords<String, Transaction> cRecord = kafkaConsumer.poll(Duration.ofSeconds(1));

            //Se verifica que efectivamente exista algun mensaje nuevo
            if(!cRecord.isEmpty()){
                for(ConsumerRecord<String,Transaction> cr: cRecord){
                    //Se envia la notificacion
                    sendUserNotification(cr.value());
                }
            }
            kafkaConsumer.commitAsync();
        }

    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();

        /**
         * Complete el código aquí para configurar el resto de los parámetros del cliente de Kafka
         */
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers/** Complete el codigo aca en caso que sea necesario **/);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() /** Complete el codigo aca en caso que sea necesario **/);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName()/** Complete el codigo aca en caso que sea necesario **/);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,consumerGroup /** Complete el codigo aca en caso que sea necesario **/);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(properties);
    }

    private static void sendUserNotification(Transaction transaction) {
        System.out.println(
                String.format("Enviando al usuario %s una notificacion acerca de una transaccion sospechosa de $%.2f en su cuenta " +
                                "procedente de %s",
                        transaction.getUser(),
                        transaction.getAmount(),
                        transaction.getTransactionLocation()));
    }
}
