import java.io.InputStream;
import java.util.*;

/**
 * Habilita un servidor HTTP que recibe transacciones de compra en tiempo real
 */
public class IncomingTransactionsReader implements Iterator<Transaction> {
    private static final String INPUT_TRANSACTIONS_FILE = "user-transactions.txt";
    private final List<Transaction> transactions;
    private final Iterator<Transaction> transactionIterator;

    public IncomingTransactionsReader(){
        this.transactions = loadTransactions();
        this.transactionIterator = transactions.iterator();
    }

    private List<Transaction> loadTransactions() {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(INPUT_TRANSACTIONS_FILE);

        Scanner scanner = new Scanner(inputStream);
        List<Transaction> transactions = new ArrayList<>();

        while (scanner.hasNextLine()) {
            String[] transaction = scanner.nextLine().split(" ");
            String user = transaction[0];
            String transactionLocation = transaction[1];
            double amount = Double.valueOf(transaction[2]);
            transactions.add(new Transaction(user, amount, transactionLocation));
        }

        return Collections.unmodifiableList(transactions);
    }

    @Override
    public boolean hasNext() {
        return transactionIterator.hasNext();
    }

    @Override
    public Transaction next() {
        return transactionIterator.next();
    }
}
