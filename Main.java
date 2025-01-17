import java.sql.*;


public class Main {

    public static void main(String[] args) {
        String databaseURL = "jdbc:ucanaccess://D:\\BelegDB (1)\\BelegDB.accdb";

        try (Connection connection = DriverManager.getConnection(databaseURL)) {

//            Measurement range of sensor ACC2:
            measurement_range(connection);

//            At which time step does the sensor with SensorID 3 reach its maximum or minimum acceleration?
            sensor_max_min(connection);

//            At how many rotations per second is the max or min power output achieved
            rotations_per_second(connection);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private static void measurement_range(Connection connection) throws SQLException {
        String sql_1 = "SELECT MIN(`ACC2 (m/s²)`) AS Min_ACC2, MAX(`ACC2 (m/s²)`) AS Max_ACC2 FROM Beschleunigung";
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sql_1);
        if (result.next()) {
            double minACC2 = result.getDouble("Min_ACC2");
            double maxACC2 = result.getDouble("Max_ACC2");
            System.out.println("Measurement range of sensor ACC2: " + minACC2 + " to " + maxACC2 + " m/s²");
        }
    }

    private static void sensor_max_min(Connection connection) throws SQLException {
        String sql_max = "SELECT Zeitschritt, `ACC3 (m/s²)` FROM Beschleunigung ORDER BY `ACC3 (m/s²)` DESC LIMIT 1";
        String sql_min = "SELECT Zeitschritt, `ACC3 (m/s²)` FROM Beschleunigung ORDER BY `ACC3 (m/s²)` ASC LIMIT 1";
        Statement statement = connection.createStatement();
        ResultSet result2 = statement.executeQuery(sql_max);
        ResultSet result3 = statement.executeQuery(sql_min);

        if (result2.next()) {
            int timeStep_max = result2.getInt("Zeitschritt");
            double acc3_max = result2.getDouble("ACC3 (m/s²)");

            System.out.println("Maximum ACC3 for SensorID 3 is at timeStep: " + timeStep_max + ", ACC3: " + acc3_max + " m/s²");
        }


        if (result3.next()) {
            int timeStep_min = result3.getInt("Zeitschritt");
            double acc3_min = result3.getDouble("ACC3 (m/s²)");
            System.out.println("Minimum ACC3 is at timeStep: " + timeStep_min + ", ACC3: " + acc3_min + " m/s²");
        }
    }

    private static void rotations_per_second(Connection connection) throws SQLException {
        String rotation_min_power = "SELECT rot.`v_rot (rot/s)` FROM Rotationen rot JOIN Leistung lei ON rot.Zeitschritt = lei.Zeitschritt ORDER BY lei.`Leistung (MW)` ASC LIMIT 1";
        String rotation_max_power = "SELECT rot.`v_rot (rot/s)` FROM Rotationen rot JOIN Leistung lei ON rot.Zeitschritt = lei.Zeitschritt ORDER BY lei.`Leistung (MW)` DESC LIMIT 1";
        Statement statement = connection.createStatement();
        ResultSet result4 = statement.executeQuery(rotation_min_power);
        ResultSet result5 = statement.executeQuery(rotation_max_power);

        if (result4.next()) {
            double rot_value = result4.getDouble("v_rot (rot/s)");
            System.out.println("Rotations per second at minimum power output: " + rot_value);
        }

        if (result5.next()) {
            double rot_value = result5.getDouble("v_rot (rot/s)");
            System.out.println("Rotations per second at maximum power output: " + rot_value);
        }
    }

}

