import java.sql.*;
import java.util.ArrayList;


public class Main {

    public static void main(String[] args) {
        String databaseURL = "jdbc:ucanaccess://D:\\BelegDB (1)\\BelegDB.accdb";

        try (Connection connection = DriverManager.getConnection(databaseURL)) {

//            Measurement range of sensor ACC2:
            String sql_1 = "SELECT MIN(`ACC2 (m/s²)`) AS Min_ACC2, MAX(`ACC2 (m/s²)`) AS Max_ACC2 FROM Beschleunigung";
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(sql_1);
            if (result.next()) {
                double minACC2 = result.getDouble("Min_ACC2");
                double maxACC2 = result.getDouble("Max_ACC2");
                System.out.println("Q2(a) Measurement range of sensor ACC2: " + minACC2 + " to " + maxACC2 + " m/s²");
            }

//            At which time step does the sensor with SensorID 3 reach its maximum or minimum acceleration?
            String sql_max = "SELECT Zeitschritt, `ACC3 (m/s²)` FROM Beschleunigung ORDER BY `ACC3 (m/s²)` DESC LIMIT 1";
            ResultSet result2 = statement.executeQuery(sql_max);

            if (result2.next()) {
                int timeStep_max = result2.getInt("Zeitschritt");
                double acc3_max = result2.getDouble("ACC3 (m/s²)");

                System.out.println("Maximum ACC3 for SensorID 3 is at timeStep: " + timeStep_max + ", ACC3: " + acc3_max + " m/s²");
            }

            String sql_min = "SELECT Zeitschritt, `ACC3 (m/s²)` FROM Beschleunigung ORDER BY `ACC3 (m/s²)` ASC LIMIT 1";
            ResultSet result3 = statement.executeQuery(sql_min);

            if (result3.next()) {
                int timeStep_min = result3.getInt("Zeitschritt");
                double acc3_min = result3.getDouble("ACC3 (m/s²)");
                System.out.println("Minimum ACC3 is at timeStep: " + timeStep_min + ", ACC3: " + acc3_min + " m/s²");
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}

