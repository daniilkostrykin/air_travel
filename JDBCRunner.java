package Practice;

import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "localhost/";

    private static final String DATABASE_NAME = "air_travel";
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "postgres";

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;


    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
           addClient(connection, "Даня2", 19999999, "2005-04-28", "М", "РФ", "my mail");
            correctClient(connection, 1, "daniilkostrykin1@gmail..com");
         //   removeClient(connection, 1);
            insertSeats(connection, 1);
            buyTicket(connection, 1, "2024-08-15 10:00:00", "2024-08-15 12:00:00", "qw", "wq", 10, 9999, "1");
            changeTicket(connection, 1, 1, "2023-08-15 10:00:00", "2023-08-15 15:00:00", "VNK", "KLN", 15, 5000, "1");
         //   returnTicket(connection,51 );
            addLuggage(connection, 1);
            changeDepartureAirport(connection, 1, "VNK");
            changeArrivalAirport(connection, 1, "C");
            delayFLight(connection, "1", "2024-08-15 10:00:00");
            cancelFlight(connection, "1");
            getClients(connection);
            System.out.println("ээээ");
            getTickets(connection);
            System.out.println("ээээ");
            System.out.println("ээээ");
            System.out.println("ээээ");
            getClientTickets(connection, "Даня");
            System.out.println("ээээ");
            getFlightsForPeriod(connection, Timestamp.valueOf("2023-08-15 10:00:00"), Timestamp.valueOf("2024-07-15 10:00:00"));
            System.out.println("ээээ");
            getFlightsFromAirport(connection, "VNK");
            System.out.println("ээээ");
            getCheapestTop(connection, 10);
            getTicketById(connection, 1);
            getClientNamed(connection, "Даня");
        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных " + e.getMessage());
            } else throw new RuntimeException(e);
        }
    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    private static void addClient(Connection connection, String full_name, long passport_series, String birth_date, String gender, String citizenship, String email) throws SQLException {
        if (!(gender.equals("М") || gender.equals("Ж")) || full_name == null || full_name.isBlank() || passport_series < 0 || birth_date.isBlank() || gender.isBlank() || citizenship.isBlank() || email.isBlank())
            return;

        java.sql.Date sqlBirthDate = java.sql.Date.valueOf(birth_date);
        PreparedStatement statement = connection.prepareStatement("INSERT INTO client(full_name, passport_series, birth_date, gender, citizenship, email) VALUES (?, ?, ?, ?, ?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);

        statement.setString(1, full_name);
        statement.setLong(2, passport_series); // Использование setLong для bigint
        statement.setDate(3, sqlBirthDate);
        statement.setString(4, gender);
        statement.setString(5, citizenship);
        statement.setString(6, email);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор клиента " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " client");
        getClients(connection);
    }

    private static void correctClient(Connection connection, int id, String email) throws SQLException {
        if (id < 0 || email.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE client SET email=? WHERE id=?;");
        statement.setString(1, email); // сначала что передаем
        statement.setInt(2, id);   // затем по чему ищем

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("UPDATEd " + count + " client(s)");
        getClients(connection);
    }

    private static void removeClient(Connection connection, int id) throws SQLException {
        if (id < 0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from client WHERE id=?;");
        statement.setInt(1, id);

        // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + statement.executeUpdate() + " client(s)");
        getClients(connection);
    }

    private static void insertSeats(Connection connection, int flight) throws SQLException {
        if (flight < 0) return;
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO seat (seat, flight) VALUES (?, ?)")) {
            for (int row = 1; row <= 32; row++) {
                for (char column = 'A'; column <= 'E'; column++) {
                    String seat = row + String.valueOf(column);   // Формируем значение места, например, "1A"
                    statement.setString(1, seat);
                    statement.setInt(2, flight);
                    statement.addBatch();
                }
            }
            System.out.println("Inserted " + statement.executeBatch().length + " seats for flight " + flight);
        }
    }

    private static void buyTicket(Connection connection, int client_id, String departure_time, String arrival_time, String departure_airport, String arrival_airport, int gate, long price, String flight) throws SQLException {

        if (!departure_airport.matches("[A-Z]{3}") || !arrival_airport.matches("[A-Z]{3}") || client_id < 0 || price < 0 ||
                departure_time.isBlank() || departure_airport.length() > 3 || arrival_airport.length() > 3 || arrival_time.isBlank() || departure_airport.isBlank() || arrival_airport.isBlank() || gate < 0 || flight.isBlank()) {
            return;
        }

        // Найдем свободное место для рейса
        PreparedStatement statement = connection.prepareStatement("SELECT id FROM seat WHERE flight = ? AND ticket_number_id IS NULL LIMIT 1");
        statement.setString(1, flight);

        ResultSet rs = statement.executeQuery();

        if (!rs.next()) {
            System.out.println("No available seats for flight " + flight);
            return;
        }

        int seatId = rs.getInt("id");
        Timestamp Sdeparture_time = Timestamp.valueOf(departure_time);
        Timestamp Sarrival_time = Timestamp.valueOf(arrival_time);
        // Вставим запись о бронировании билета
        PreparedStatement statement1 = connection.prepareStatement("INSERT INTO ticket(client_id, flight, departure_time, arrival_time, departure_airport, arrival_airport, gate, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
        statement1.setInt(1, client_id);
        statement1.setString(2, flight);
        statement1.setTimestamp(3, Sdeparture_time);
        statement1.setTimestamp(4, Sarrival_time);
        statement1.setString(5, departure_airport);
        statement1.setString(6, arrival_airport);
        statement1.setInt(7, gate);
        statement1.setLong(8, price);

        if (statement1.executeUpdate() > 0) {
            ResultSet generatedKeys = statement1.getGeneratedKeys();
            if (generatedKeys.next()) {
                int bookingId = generatedKeys.getInt(1);
                // Обновим информацию о месте
                PreparedStatement statement2 = connection.prepareStatement("UPDATE seat SET ticket_number_id = ? WHERE id = ?");
                statement2.setInt(1, bookingId);
                statement2.setInt(2, seatId);
                statement2.executeUpdate();

                System.out.println("Inserted ticket with seat ID " + seatId);
            }
        } else {
            System.out.println("No tickets inserted");
        }
        PreparedStatement statement3 = connection.prepareStatement("UPDATE ticket SET seat = s.seat FROM seat s WHERE s.ticket_number_id = ticket.id;");
        statement3.executeUpdate();

    }

    private static void changeTicket(Connection connection, int ticket_id, int client_id, String departure_time, String arrival_time, String departure_airport, String arrival_airport, int gate, long price, String flight) throws SQLException {
        if (!departure_airport.matches("[A-Z]{3}") || !arrival_airport.matches("[A-Z]{3}") || ticket_id < 0
                || client_id < 0 || price < 0 || departure_time.isBlank() || departure_airport.length() > 3 || arrival_airport.length() > 3
                || arrival_time.isBlank() || departure_airport.isBlank() || arrival_airport.isBlank() || gate < 0 || flight.isBlank()) {
            return;
        }
        PreparedStatement statement = connection.prepareStatement("DELETE FROM ticket WHERE id = ?;");
        PreparedStatement statement1 = connection.prepareStatement("UPDATE seat SET ticket_number_id = ? WHERE ticket_number_id = ?");
        statement.setInt(1, ticket_id);
        statement1.setInt(1, ticket_id);
        statement1.setInt(2, ticket_id);
        statement.executeUpdate();
        statement1.executeUpdate();
        buyTicket(connection, client_id, departure_time, arrival_time, departure_airport, arrival_airport, gate, price, flight);
        System.out.println("Changed ticket with ID " + ticket_id);
    }

    private static void returnTicket(Connection connection, int ticket_id) throws SQLException {
        if (ticket_id < 0) return;
        PreparedStatement statement = connection.prepareStatement("DELETE FROM ticket WHERE id = ?;\n");
        statement.setInt(1, ticket_id);
        PreparedStatement statement1 = connection.prepareStatement("UPDATE seat SET ticket_number_id = NULL WHERE ticket_number_id = ?");
        statement1.setInt(1, ticket_id);
        statement.executeUpdate();
        statement1.executeUpdate();
    }

    private static void addLuggage(Connection connection, int ticket_id) throws SQLException {
        if (ticket_id < 0) return;
        PreparedStatement statement = connection.prepareStatement("UPDATE ticket SET luggage = 'yes' WHERE id = ?");
        statement.setInt(1, ticket_id);
        statement.executeUpdate();
        System.out.println("Added luggage for ID" + ticket_id);
    }

    private static void changeDepartureAirport(Connection connection, int ticket_id, String departure_airport) throws SQLException {
        if (ticket_id < 0 || !departure_airport.matches("[A-Z]{3}")) return;
        PreparedStatement statement = connection.prepareStatement("UPDATE ticket SET departure_airport = ? WHERE id = ?");
        statement.setString(1, departure_airport);
        statement.setInt(2, ticket_id);
        statement.executeUpdate();
        System.out.println("Changed departure airport for ID" + ticket_id);
    }

    private static void changeArrivalAirport(Connection connection, int ticket_id, String arrival_airport) throws SQLException {
        if (!arrival_airport.matches("[A-Z]{3}")) return;
        PreparedStatement statement = connection.prepareStatement("UPDATE ticket SET arrival_airport = ? WHERE id = ?");
        statement.setString(1, arrival_airport);
        statement.setInt(2, ticket_id);
        statement.executeUpdate();
        System.out.println("Changed arrival airport for ID" + ticket_id);
    }

    private static void delayFLight(Connection connection, String flight, String departure_time) throws SQLException {
        if (departure_time.isBlank()) return;
        Timestamp Sdeparture_time = Timestamp.valueOf(departure_time);
        PreparedStatement statement = connection.prepareStatement("UPDATE ticket SET flight_status = 'DELAYED', departure_time = ? WHERE flight = ?");
        statement.setTimestamp(1, Sdeparture_time);
        statement.setString(2, flight);
        statement.executeUpdate();
        System.out.println("Delayed flight for ID" + flight);

    }

    private static void cancelFlight(Connection connection, String flight) throws SQLException {
        if (flight.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("UPDATE ticket SET flight_status = 'CANCELLED' WHERE flight = ?");
        statement.setString(1, flight);
        statement.executeUpdate();
        System.out.println("Cancelled flight for ID" + flight);
    }

    private static void getClients(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "id", columnName1 = "full_name", columnName2 = "passport_series", columnName3 = "birth_date", columnName4 = "gender", columnName5 = "citizenship", columnName6 = "email";
        // значения ячеек
        int param0;
        long param2;
        String param1, param3, param4, param5, param6;

        Statement statement = connection.createStatement(); // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM client;"); // выполняем запрос на поиск и получаем список ответов
        System.out.println(columnName0 + " | " + columnName1 + " | " + columnName2 + " | " + columnName3 + " | " + columnName4 + " | " + columnName5 + " | " + columnName6);
        while (rs.next()) { // пока есть данные, продвигаться по ним
            param6 = rs.getString(columnName6);
            param5 = rs.getString(columnName5);
            param4 = rs.getString(columnName4);
            param3 = rs.getString(columnName3);
            param2 = rs.getLong(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0); // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6);
        }
    }

    private static void getTickets(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "client_id", columnName2 = "flight", columnName3 = "departure_time", columnName4 = "arrival_time", columnName5 = "departure_airport", columnName6 = "arrival_airport", columnName7 = "gate", columnName8 = "price", columnName9 = "seat", columnName10 = "luggage", columnName11 = "flight_status";

        int param0, param1, param7;
        long param8;
        Timestamp param3, param4;
        String param2, param5, param6, param9, param10, param11;

        Statement statement = connection.createStatement(); // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM ticket;"); // выполняем запрос на поиск и получаем список ответов

        // Выводим заголовок столбцов
        System.out.println(columnName0 + " | " + columnName1 + " | " + columnName2 + " | " + columnName3 + " | " + columnName4 + " | " + columnName5 + " | " + columnName6 + " | " + columnName7 + " | " + columnName8 + " | " + columnName9 + " | " + columnName10 + " | " + columnName11);

        while (rs.next()) { // пока есть данные, продвигаться по ним
            param0 = rs.getInt(columnName0);
            param1 = rs.getInt(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getTimestamp(columnName3);
            param4 = rs.getTimestamp(columnName4);
            param5 = rs.getString(columnName5);
            param6 = rs.getString(columnName6);
            param7 = rs.getInt(columnName7);
            param8 = rs.getLong(columnName8);
            param9 = rs.getString(columnName9);
            param10 = rs.getString(columnName10);
            param11 = rs.getString(columnName11);

            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7 + " | " + param8 + " | " + param9 + " | " + param10 + " | " + param11);
        }
    }

    private static void getClientNamed(Connection connection, String firstName) throws SQLException {
        if (firstName == null || firstName.isBlank()) return; // проверка "на дурака"

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT id, full_name, passport_series, birth_date " +
                        "FROM client " +
                        "WHERE full_name LIKE ?");
        statement.setString(1, firstName + "%");

        ResultSet rs = statement.executeQuery();

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) + " | " + rs.getString(4));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getClientTickets(Connection connection, String full_name) throws SQLException {
        if (full_name == null || full_name.isBlank()) return; // проверка "на дурака"
        full_name = '%' + full_name + '%'; // переданное значение может быть дополнено сначала и в конце (часть слова)

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement("SELECT client.full_name, ticket.id, seat.seat " + "FROM client " + "JOIN ticket ON client.id = ticket.client_id " + // Соединяем client и booking_tickets по client.id
                "JOIN seat ON seat.ticket_number_id = ticket.id " + // Соединяем seat и booking_tickets по booking_number_id
                "WHERE client.full_name LIKE ?;"); // создаем оператор шаблонного-запроса с "включаемыми" параметрами
        statement.setString(1, full_name); // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery(); // выполняем запрос на поиск и получаем список ответов

        while (rs.next()) { // пока есть данные, перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getFlightsForPeriod(Connection connection, Timestamp departure_time_start, Timestamp departure_time_finish) throws SQLException {
        if (departure_time_start == null || departure_time_finish == null || departure_time_finish.before(departure_time_start))
            return;
        PreparedStatement statement = connection.prepareStatement("SELECT flight FROM ticket WHERE departure_time BETWEEN ? AND ?");
        statement.setTimestamp(1, departure_time_start);
        statement.setTimestamp(2, departure_time_finish);
        ResultSet rs = statement.executeQuery();
        int cnt = 0;
        while (rs.next()) {
            if (rs.getString("flight") != null) {
                cnt++;
            }
        }
        System.out.println("Количество рейсов: " + cnt);
    }

    private static void getFlightsFromAirport(Connection connection, String airport) throws SQLException {
        if (!airport.matches("[A-Z]{3}") || airport.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("SELECT flight FROM ticket WHERE departure_airport = ?");
        statement.setString(1, airport);
        ResultSet rs = statement.executeQuery();
        int cnt = 0;
        while (rs.next()) {
            if (rs.getString("flight") != null) {
                cnt++;
            }
        }
        System.out.println(cnt);
    }

    private static void getCheapestTop(Connection connection, int digit) throws SQLException {
        if (digit <= 0) return;
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM public.ticket ORDER BY price ASC LIMIT ?");
        statement.setInt(1, digit);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println("ID: " + rs.getString("id") + ", Departure airport: " + rs.getString("departure_airport") + ", Arrival airport: " + rs.getString("arrival_airport") + ", Price: " + rs.getString("price"));
        }
    }

    private static void getTicketById(Connection connection, int ticket_id) throws SQLException {
        if (ticket_id <= 0) return;
        String columnName0 = "id", columnName1 = "client_id", columnName2 = "flight", columnName3 = "departure_time", columnName4 = "arrival_time", columnName5 = "departure_airport", columnName6 = "arrival_airport", columnName7 = "gate", columnName8 = "price", columnName9 = "seat", columnName10 = "luggage", columnName11 = "flight_status";

        int param0, param1, param7;
        long param8;
        Timestamp param3, param4;
        String param2, param5, param6, param9, param10, param11;

        PreparedStatement statement = connection.prepareStatement("SELECT * FROM ticket WHERE id = ?");
        statement.setInt(1, ticket_id);
        ResultSet rs = statement.executeQuery();

        System.out.println(columnName0 + " | " + columnName1 + " | " + columnName2 + " | " + columnName3 + " | " + columnName4 + " | " + columnName5 + " | " + columnName6 + " | " + columnName7 + " | " + columnName8 + " | " + columnName9 + " | " + columnName10 + " | " + columnName11);

        boolean found = false;

        while (rs.next()) {
            param0 = rs.getInt(columnName0);
            param1 = rs.getInt(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getTimestamp(columnName3);
            param4 = rs.getTimestamp(columnName4);
            param5 = rs.getString(columnName5);
            param6 = rs.getString(columnName6);
            param7 = rs.getInt(columnName7);
            param8 = rs.getLong(columnName8);
            param9 = rs.getString(columnName9);
            param10 = rs.getString(columnName10);
            param11 = rs.getString(columnName11);

            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5 + " | " + param6 + " | " + param7 + " | " + param8 + " | " + param9 + " | " + param10 + " | " + param11);
            found = true;
        }

        if (!found) {
            System.out.println("Ticket with ID " + ticket_id + " was not found.");
        }
    }
}
