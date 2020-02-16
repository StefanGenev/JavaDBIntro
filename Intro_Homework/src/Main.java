import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class Main {
    private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/";
    private static final String DATABASE_NAME = "minions_db";

    private static Connection connection;
    private static String query;
    private static PreparedStatement statement;
    private static BufferedReader reader;
    public static void main(String[] args) throws SQLException, IOException {
        reader = new BufferedReader(new InputStreamReader(System.in));

        Properties props = new Properties();
        //TODO: CHANGE PROPERTIES BEFORE TESTING
        props.setProperty("user","root");
        props.setProperty("password","root");

        connection =
                DriverManager.getConnection(CONNECTION_STRING + DATABASE_NAME,props);

        boolean flag = true;
        while (flag) {
            System.out.println("Select exercise from 2 to 9 or type 10 to exit.");
            System.out.printf("Exercise number: ");
            int exerciseNumber = Integer.parseInt(reader.readLine());
            System.out.println();
            switch (exerciseNumber) {
                case 2:
                    System.out.println("2. Get Villains’ Names");
                    getVillainsNamesAndCountOfMinions();
                    break;
                case 3:
                    System.out.println("3. Get Minion Names");
                    getMinionsByVillainId();
                    break;
                case 4:
                    System.out.println("4. Add Minion");
                    addMinionsToDatabase();
                    break;
                case 5:
                    System.out.println("5. Change Town Names Casing");
                    changeTownNamesToUpperCase();
                    break;
                case 6:
                    System.out.println("6. *Remove Villain");
                    removeVillainByIdAndFreeMinions();
                    break;
                case 7:
                    System.out.println("7. Print All Minion Names");
                    printAllMinionNames();
                    break;
                case 8:
                    System.out.println("8. Increase Minions Age");
                    increaseMinionsAge();
                    break;
                case 9:
                    System.out.println("9. Increase Age Stored Procedure");
                    increaseAgeWithStoredProcedure();
                    break;
                default:
                    flag = false;
                    break;
            }
            System.out.println();
        }

    }

    private static void increaseAgeWithStoredProcedure() throws IOException, SQLException {
        System.out.println("Enter minion ID: ");
        int minionId = Integer.parseInt(reader.readLine());

        CallableStatement procedure = connection.prepareCall("call usp_get_older(?)");
        procedure.setInt(1,minionId);

        procedure.executeUpdate();

        query = "SELECT name,age FROM minions WHERE id = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1,minionId);
        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            System.out.printf("%s %d%n",resultSet.getString(1),resultSet.getInt(2));
        }


    }

    private static void increaseMinionsAge() throws IOException, SQLException {
        System.out.println("Enter minion IDs: ");
        String[] idList = reader.readLine().split("\\s+");

        for (int i = 0; i < idList.length; i++) {
            int currId = Integer.parseInt(idList[i]);
            if (checkIfEntityExists(currId,"minions")) {
                query = "UPDATE minions\n" +
                        "SET age = age + 1\n" +
                        "WHERE id = ?";
                statement = connection.prepareStatement(query);
                statement.setInt(1,currId);

                statement.executeUpdate();

                query = "UPDATE minions\n" +
                        "SET name = lcase(name)\n" +
                        "WHERE id = ?";
                statement = connection.prepareStatement(query);
                statement.setInt(1,currId);

                statement.executeUpdate();
            }
        }

        query = "SELECT name,age FROM minions";
        statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            System.out.printf("%s %d%n",resultSet.getString(1),resultSet.getInt(2));
        }
    }

    private static void printAllMinionNames() throws SQLException {
        query = "SELECT name FROM minions";
        statement = connection.prepareStatement(query);

        ResultSet resultSet = statement.executeQuery();
        ArrayList<String> minionNames = new ArrayList<>();
        while (resultSet.next()) {
            minionNames.add(resultSet.getString(1));
        }
        for (int i = 0; i <minionNames.size()/2 ; i++) {
            System.out.printf("%s%n",minionNames.get(i));
            System.out.printf("%s%n",minionNames.get(minionNames.size()-1-i));
        }
    }

    private static void removeVillainByIdAndFreeMinions() throws IOException, SQLException {
        System.out.println("Enter villain id: ");
        int villainId = Integer.parseInt(reader.readLine());
        String villainName = getEntityNameById(villainId,"villains");

        if (!checkIfEntityExists(villainId,"villains")) {
            System.out.println("No such villain was found");
            return;
        }

        query = "SELECT * from minions_villains\n" +
                "where villain_id = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1,villainId);
        ResultSet resultSet = statement.executeQuery();

        int counter = 0;
        while (resultSet.next()) {
            counter++;
        }

        query = "DELETE FROM minions_villains\n" +
                "WHERE villain_id = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1,villainId);
        statement.execute();

        query = "DELETE FROM villains\n" +
                "WHERE id = ?";
        statement = connection.prepareStatement(query);
        statement.setInt(1,villainId);
        statement.execute();
        System.out.printf("%s was deleted\n" +
                "%d minions released",villainName,counter);

    }

    private static void changeTownNamesToUpperCase() throws IOException, SQLException {
        System.out.println("Enter country: ");
        String country = reader.readLine();

        query = "update towns\n" +
                "set name = ucase(name)\n" +
                "where country = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1,country);

        statement.executeUpdate();

        query = "SELECT *\n" +
                "FROM towns\n" +
                "WHERE country = ?";
        statement = connection.prepareStatement(query);
        statement.setString(1,country);
        ResultSet resultSet = statement.executeQuery();

        int counter = 0;
        ArrayList<String> townsList = new ArrayList<>();
        while (resultSet.next()) {
            townsList.add(resultSet.getString("name"));
            counter++;
        }
        if (counter>0) {
            System.out.printf("%d town names were affected.%n" + townsList,counter);
        } else {
            System.out.println("No town names were affected.");
        }

    }

    private static void addMinionsToDatabase() throws IOException, SQLException {
        System.out.println("Enter minion parameters: ");
        String[] arg = reader.readLine().split("\\s+");
        String minionName = arg[0];
        int minionAge = Integer.parseInt(arg[1]);
        String minionTown = arg[2];

        System.out.println("Enter villain name: ");
        String villainName = reader.readLine();

        if (!checkIfEntityExistsByName(minionTown,"towns")) {
            addTownToTable(minionTown);

        }
        if (!checkIfEntityExistsByName(villainName,"villains")) {
            addVillainToTable(villainName);
        }

        if (!checkIfEntityExistsByName(minionName,"minions")) {
            addMinionToTable(minionName,minionAge,getEntityIdByName(minionTown,"towns"));
        }
        addMinionAndVillainToTable(getEntityIdByName(minionName,"minions"),getEntityIdByName(villainName,"villains"));
        System.out.printf("Successfully added %s to be minion of %s.%n",minionName,villainName);
    }

    private static void addMinionAndVillainToTable(int minionId,int villainId) throws SQLException {
        query = "INSERT INTO minions_villains(minion_id,villain_id) VALUES (? , ?)";
        statement = connection.prepareStatement(query);
        statement.setInt(1,minionId);
        statement.setInt(2,villainId);

        statement.execute();
    }

    private static void addTownToTable(String townName) throws SQLException {
        query = "Insert into towns(name) values (?)";
        statement = connection.prepareStatement(query);
        statement.setString(1,townName);

        statement.execute();
        System.out.printf("Town %s was added to the database.%n",townName);
    }

    private static void addVillainToTable(String first_value) throws SQLException {

        query = "Insert into villains(name,evilness_factor) values (? , 'evil')";
        statement = connection.prepareStatement(query);
        statement.setString(1,first_value);

        statement.execute();
        System.out.printf("Villain %s was added to the database.%n",first_value);
    }

    private static void addMinionToTable(String name,int age,int town_id) throws SQLException {

        query = "Insert into minions(name,age,town_id) values (? , ? , ?)";
        statement = connection.prepareStatement(query);
        statement.setString(1,name);
        statement.setInt(2,age);
        statement.setInt(3,town_id);

        statement.execute();
    }

    private static boolean checkIfEntityExists(int id,String table) throws SQLException {
        query = "SELECT * FROM " + table + " WHERE id = ?";

        statement = connection.prepareStatement(query);
        statement.setInt(1,id);

        ResultSet rs = statement.executeQuery();

        return rs.next();
    }

    private static boolean checkIfEntityExistsByName(String name, String table) throws SQLException {
        query = "SELECT * FROM " + table + " WHERE name = ?";

        statement = connection.prepareStatement(query);
        statement.setString(1,name);

        ResultSet rs = statement.executeQuery();

        return rs.next();
    }

    private static void getVillainsNamesAndCountOfMinions() throws SQLException {
        query = "SELECT v.name, COUNT(mv.minion_id) as 'count'\n" +
                "FROM villains as v\n" +
                "JOIN minions_villains mv on v.id = mv.villain_id\n" +
                "GROUP BY v.name\n" +
                "HAVING `count` > 15\n" +
                "ORDER BY `count` DESC";

        statement = connection.prepareStatement(query);

        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            System.out.printf("%s %s %n",resultSet.getString(1),resultSet.getInt(2));
        }

    }

    private static void getMinionsByVillainId() throws SQLException, IOException {
        System.out.println("Enter villain id: ");
        int inputId = Integer.parseInt(reader.readLine());


        if (!checkIfEntityExists(inputId,"villains")) {
            System.out.printf("No villain with ID %s exists in the database.",inputId);
            return;
        }
        System.out.printf("Villain: %s%n",getEntityNameById(inputId,"villains"));

        getMinionsAndAgeByVillainId(inputId);

    }

    private static void getMinionsAndAgeByVillainId(int villain_id) throws SQLException {
        query = "SELECT m.id, m.name,m.age\n" +
                "FROM minions_villains as mv\n" +
                "JOIN minions m on mv.minion_id = m.id\n" +
                "JOIN villains v on mv.villain_id = v.id\n" +
                "WHERE mv.villain_id = ?\n";

        statement = connection.prepareStatement(query);

        statement.setInt(1,villain_id);
        ResultSet resultSet = statement.executeQuery();

        int minionNumber = 1;

        while (resultSet.next()) {
            System.out.printf("%d. %s %d%n",
                    minionNumber,resultSet.getString(2),resultSet.getInt(3));
            minionNumber++;
        }
    }

    private static String getEntityNameById(int entityId,String tableName) throws SQLException {
        query = "SELECT name FROM " + tableName +" WHERE id = ?" ;

        statement = connection.prepareStatement(query);

        statement.setInt(1,entityId);
        ResultSet rs = statement.executeQuery();
        return rs.next() ? rs.getString("name") : null;
    }

    private static int getEntityIdByName(String entityName,String tableName) throws SQLException {
        query = "SELECT id FROM " + tableName +" WHERE name = ? LIMIT 1" ;

        statement = connection.prepareStatement(query);

        statement.setString(1,entityName);
        ResultSet rs = statement.executeQuery();
        return rs.next()? rs.getInt("id") : null;
    }
}
