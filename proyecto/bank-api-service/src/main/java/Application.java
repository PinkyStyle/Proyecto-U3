import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Servicio de API Bancaria
 */
public class Application {
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";

    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        Producer<String, Transaction> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            processTransactions(new IncomingTransactionsReader(), new UserResidenceDatabase(), kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                           UserResidenceDatabase userResidenceDatabase,
                                           Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {

        String realLocation,transLocation;
        while (incomingTransactionsReader.hasNext()) {
            Transaction transaction = incomingTransactionsReader.next();
            /**
             * Complete el código en caso que sea necesario.
             * Envie la transacción al tema(topic) correcto, según el origen de la transaccion y los datos de residencia del usuario
             */
            realLocation = userResidenceDatabase.getUserResidence(transaction.getUser());
            transLocation = transaction.getTransactionLocation();

            if(realLocation.equals(transLocation)){
                //lo enviamos al topic valid-transactions
                ProducerRecord<String, Transaction> tRecord = new ProducerRecord<>(VALID_TRANSACTIONS_TOPIC,transaction.getUser(),transaction);

                //enviar el record mediante el producer creado
                RecordMetadata rmd = kafkaProducer.send(tRecord).get();

                //imprimir datos del mensaje
                System.out.println("Transaccion "+ transaction.toString() + " CORRECTA");

            }
            else{
                //lo enviamos al topic suspicious-transactions
                ProducerRecord<String, Transaction> tRecord = new ProducerRecord<>(SUSPICIOUS_TRANSACTIONS_TOPIC,transaction.getUser(), transaction);

                //enviar el record mediante el producer creado
                RecordMetadata rmd = kafkaProducer.send(tRecord).get();

                //imprimir datos del mensaje
                System.out.println("Transaccion "+ transaction.toString() + "SOSPECHOSA");

            }
        }
    }

    public static Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();

        /*
        Aqui cambie cosas
         */
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "banking-api-service");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class /** Complete el código en caso que sea necesario **/);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName() /** Complete el código en caso que sea necesario **/);

        return new KafkaProducer<>(properties);
    }

}
