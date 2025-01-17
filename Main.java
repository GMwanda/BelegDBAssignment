import java.sql.*;
import java.util.ArrayList;


public class Main {

    public static void main(String[] args) {
        String databaseURL = "jdbc:ucanaccess://D:\\BelegDB (1)\\BelegDB.accdb";

        try (Connection connection = DriverManager.getConnection(databaseURL)) {

            measurement_range(connection);

            sensor_max_min(connection);

            rotations_per_second(connection);

            acc_acc1_method1(connection);

            acc_acc1_method2(connection, "00:00:21", 15);

            rotations_list_method1(connection);

            rotations_list_method2(connection, "33", "53");
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

    private static void acc_acc1_method1(Connection connection) throws SQLException {
        String query = "SELECT `ACC1 (m/s²)` FROM Beschleunigung WHERE Zeit>= ? AND Zeit<?";
        PreparedStatement preStatement = connection.prepareStatement(query);
        preStatement.setString(1, "00:00:21");
        preStatement.setString(2, "00:00:36");

        ResultSet result6 = preStatement.executeQuery();

        ArrayList<Double> accValues = new ArrayList<>();
        while (result6.next()) {
            accValues.add(result6.getDouble("ACC1 (m/s²)"));
        }
        System.out.println("Acceleration Method 1::");
        System.out.println("Acceleration values for 15 seconds from timestamp 00:00:21 : " + accValues);
    }

    private static void acc_acc1_method2(Connection connection, String startTime, int duration) throws SQLException {
        String query = "SELECT `ACC1 (m/s²)` FROM Beschleunigung WHERE Zeit>= ? AND Zeit<?";
        PreparedStatement preStatement = connection.prepareStatement(query);
        preStatement.setString(1, startTime);
        preStatement.setString(2, incrementTime(startTime, duration));
        ResultSet result7 = preStatement.executeQuery();


        ArrayList<Double> accValues = new ArrayList<>();
        while (result7.next()) {
            accValues.add(result7.getDouble("ACC1 (m/s²)"));
        }
        System.out.println("Acceleration Method 2::");
        System.out.println("Acceleration values for 15 seconds from timestamp 00:00:21 : " + accValues);
    }

    private static String incrementTime(String startTime, int duration) {
        String[] parts = startTime.split(":");
        int hours = Integer.parseInt(parts[0]);
        int minutes = Integer.parseInt(parts[1]);
        int secs = Integer.parseInt(parts[2]);

        secs += duration;
        minutes += secs / 60;
        secs %= 60;
        hours += minutes / 60;
        minutes %= 60;

        return String.format("%02d:%02d:%02d", hours, minutes, secs);
    }

    private static void rotations_list_method1(Connection connection) throws SQLException {
        String query = "SELECT `v_rot (rot/s)` FROM Rotationen WHERE Zeitschritt>=? AND Zeitschritt<?";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setString(1, "33");
        preparedStatement.setString(2, "53");

        ResultSet result8 = preparedStatement.executeQuery();

        ArrayList<Double> rotationValues = new ArrayList<>();

        while (result8.next()) {
            rotationValues.add(result8.getDouble("v_rot (rot/s)"));
        }
        System.out.println("Rotations Method 1::");
        System.out.println("Rotations values from time step 33 - 52 : " + rotationValues);

    }

    private static void rotations_list_method2(Connection connection, String start, String end) throws SQLException {
        String query = "SELECT `v_rot (rot/s)` FROM Rotationen WHERE Zeitschritt>=? AND Zeitschritt<?";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setString(1, start);
        preparedStatement.setString(2, end);
        ResultSet result9 = preparedStatement.executeQuery();

        ArrayList<Double> rotValues = new ArrayList<>();

        while (result9.next()) {
            rotValues.add(result9.getDouble("v_rot (rot/s)"));
        }

        System.out.println("Rotations Method 2::");
        System.out.println("Rotations values from time step " + start + " - " + end + " : " + rotValues);
    }

}

