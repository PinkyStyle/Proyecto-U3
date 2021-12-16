
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Habilita una base de datos que enlaza un usuario con su país de residencia
 */
public class UserResidenceDatabase {
    private static final String USER_RESIDENCE_FILE = "user-residence.txt";
    private final Map<String, String> userToResidenceMap;

    public UserResidenceDatabase(){
        this.userToResidenceMap = loadUsersResidenceFromFile();
    }

    /**
     * Devuelve el país de residencia del usuario.
     */
    public String getUserResidence(String user) {
        if (!userToResidenceMap.containsKey(user)) {
            throw new RuntimeException("Usuario " + user + " no existe");
        }

        return userToResidenceMap.get(user);
    }

    private Map<String, String> loadUsersResidenceFromFile() {
        Map<String, String> userToResidence = new HashMap<>();

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(USER_RESIDENCE_FILE);

        Scanner scanner = new Scanner(inputStream);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String []userResidencePair = line.split(" ");
            userToResidence.put(userResidencePair[0], userResidencePair[1]);
        }
        return Collections.unmodifiableMap(userToResidence);
    }


}
