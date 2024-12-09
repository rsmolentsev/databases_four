package org.example;

import java.sql.*;
import java.time.LocalDate;

public class LibraryTransactionManager {
    // Параметры подключения к базе данных Oracle
    private static final String DB_URL = "jdbc:oracle:thin:@localhost:1521:XE";
    private static final String USERNAME = "SMOLENTSEV";
    private static final String PASSWORD = "203";

    public static void main(String[] args) {
        // Демонстрация атомарной транзакции выдачи книг
        demonstrateBookLoanTransaction("beemovie@yahoo.com", "Barry", "Benson","1984");

        // Демонстрация изолированной транзакции обновления информации о читателе
        demonstrateReaderUpdateTransaction("beemovie@yahoo.com");
    }

    /**
     * Атомарная транзакция выдачи книги
     *
     * Ключевые характеристики атомарности:
     * - Либо все операции выполняются успешно, либо ни одна не применяется
     * - Предотвращение частичного обновления данных
     * - Гарантия целостности состояния базы данных
     *
     * @param readerEmail электронная почта читателя
     * @param firstName имя читателя
     * @param lastName фамилия читателя
     * @param bookTitle название книги
     */
    private static void demonstrateBookLoanTransaction(String readerEmail, String firstName, String lastName, String bookTitle) {
        Connection connection = null;
        try {
            // Устанавливаем подключение
            connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

            // Отключаем автоматическую фиксацию транзакций
            connection.setAutoCommit(false);

            try (PreparedStatement checkBookAvailable = connection.prepareStatement(
                    "SELECT COUNT(*) FROM Book WHERE Title = ?");
                 PreparedStatement insertLoan = connection.prepareStatement(
                         "INSERT INTO Loan (LoanInfo, LoanName, LoanDate, FirstName, LastName, Email, ReturnDate) " +
                                 "SELECT ?, ?, ?, FirstName, LastName, ?, ? FROM Reader WHERE Email = ?");
                 PreparedStatement insertLoanItem = connection.prepareStatement(
                         "INSERT INTO LoanItem (LoanName, Title, DueDate) VALUES (?, ?, ?)")) {

                // Проверяем доступность книги
                checkBookAvailable.setString(1, bookTitle);
                ResultSet rs = checkBookAvailable.executeQuery();
                rs.next();
                int bookCount = rs.getInt(1);

                if (bookCount > 0) {
                    // Генерируем уникальный идентификатор займа
                    String loaner = firstName + lastName;
                    String loanName = loaner + "_loaned_books_" + "2";
                    String loanInfo = loaner + "_loan_" + "2";

                    // Даты займа и возврата
                    LocalDate loanDate = LocalDate.now();
                    LocalDate returnDate = loanDate.plusMonths(1);
                    LocalDate dueDate = loanDate.plusWeeks(2);

                    // Вставляем информацию о займе
                    insertLoan.setString(1, loanInfo);
                    insertLoan.setString(2, loanName);
                    insertLoan.setDate(3, Date.valueOf(loanDate));
                    insertLoan.setString(4, readerEmail);
                    insertLoan.setDate(5, Date.valueOf(returnDate));
                    insertLoan.setString(6, readerEmail);
                    insertLoan.executeUpdate();

                    // Вставляем информацию о книге в заем
                    insertLoanItem.setString(1, loanName);
                    insertLoanItem.setString(2, bookTitle);
                    insertLoanItem.setDate(3, Date.valueOf(dueDate));
                    insertLoanItem.executeUpdate();

                    // Фиксируем транзакцию
                    connection.commit();
                    System.out.println("Книга " + bookTitle + " успешно выдана читателю "
                            + firstName + " " + lastName + " с почтовым адресом " + readerEmail + "\n");
                } else {
                    System.out.println("Книга " + bookTitle + " недоступна для выдачи.");
                    connection.rollback();
                }
            } catch (SQLException e) {
                // В случае любой ошибки откатываем все изменения
                connection.rollback();
                System.err.println("Ошибка при выполнении транзакции выдачи книги: " + e.getMessage());
            }
        } catch (SQLException e) {
            System.err.println("Ошибка подключения к базе данных: " + e.getMessage());
        } finally {
            try {
                if (connection != null) connection.close();
            } catch (SQLException e) {
                System.err.println("Ошибка закрытия соединения: " + e.getMessage());
            }
        }
    }

    /**
     * Транзакция обновления данных читателя с высоким уровнем изоляции
     *
     * Уровень изоляции: SERIALIZABLE (Сериализуемый)
     *
     * Обоснование выбора уровня изоляции:
     * - Предотвращение всех возможных аномалий параллельных транзакций
     * - Гарантия последовательного выполнения транзакций
     * - Максимальная защита от конкурентных изменений
     *
     * Компромиссы:
     * - Strongest data integrity (Самая строгая целостность данных)
     * - Потенциальное снижение производительности
     * - Повышенная вероятность конфликтов транзакций
     */
    private static void demonstrateReaderUpdateTransaction(String readerEmail) {
        Connection connection = null;
        try {
            // Устанавливаем подключение
            connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

            // Устанавливаем максимальный уровень изоляции
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            connection.setAutoCommit(false);

            try (PreparedStatement selectReader = connection.prepareStatement(
                    "SELECT * FROM Reader WHERE Email = ? FOR UPDATE");
                 PreparedStatement updateReader = connection.prepareStatement(
                         "UPDATE Reader SET Phone = ?, Address = ? WHERE Email = ?")) {

                // Блокируем строку для обновления
                selectReader.setString(1, readerEmail);
                ResultSet rs = selectReader.executeQuery();

                if (rs.next()) {
                    String currentPhone = rs.getString("Phone");
                    String currentAddress = rs.getString("Address");

                    // Новые данные для обновления
                    String newPhone = "+7 (999) 123-45-67";
                    String newAddress = "Новая ул., д. 42";

                    // Обновляем информацию о читателе

                    updateReader.setString(1, newPhone);
                    updateReader.setString(2, newAddress);
                    updateReader.setString(3, readerEmail);
                    updateReader.executeUpdate();

                    // Фиксируем транзакцию
                    connection.commit();
                    System.out.println("Информация о читателе обновлена:");
                    System.out.println("Старый телефон: " + currentPhone + ", новый: " + newPhone);
                    System.out.println("Старый адрес: " + currentAddress + ", новый: " + newAddress);
                } else {
                    System.out.println("Читатель с email " + readerEmail + " не найден.");
                    connection.rollback();
                }
            } catch (SQLException e) {
                // Откатываем транзакцию в случае ошибки
                connection.rollback();
                System.err.println("Ошибка при обновлении информации о читателе: " + e.getMessage());
            }
        } catch (SQLException e) {
            System.err.println("Ошибка подключения к базе данных: " + e.getMessage());
        } finally {
            try {
                if (connection != null) connection.close();
            } catch (SQLException e) {
                System.err.println("Ошибка закрытия соединения: " + e.getMessage());
            }
        }
    }
}
