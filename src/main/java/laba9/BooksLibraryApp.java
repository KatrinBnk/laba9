package laba9;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.InputMismatchException;

public class BooksLibraryApp {
    private static final String DB_URL = "jdbc:derby://localhost:1527/databases/BooksAppDB;";
    private static Connection conn;
    private static final Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8);

    // Значения по умолчанию для не ключевых полей
    private static final int DEFAULT_FLOOR = 1;
    private static final int DEFAULT_CABINET = 0;
    private static final int DEFAULT_SHELF = 0;
    private static final String DEFAULT_AUTHOR = "Неизвестный автор";
    private static final String DEFAULT_TITLE = "Без названия";
    private static final String DEFAULT_PUBLISHER = "Неизвестное издательство";
    private static final int DEFAULT_PUBLICATION_YEAR = 2000;
    private static final int DEFAULT_PAGE_COUNT = 100;
    private static final int DEFAULT_WRITING_YEAR = 2000;
    private static final int DEFAULT_WEIGHT_GRAMS = 300;

    public static void main(String[] args) {
        try {
            conn = DriverManager.getConnection(DB_URL);
            System.out.println("Подключение к базе данных успешно установлено.");

            boolean exit = false;
            while (!exit) {
                System.out.println("\nГлавное меню:");
                System.out.println("1. Просмотреть все полки");
                System.out.println("2. Просмотреть все книги");
                System.out.println("3. Добавить полку");
                System.out.println("4. Добавить книгу");
                System.out.println("5. Изменить полку");
                System.out.println("6. Изменить книгу");
                System.out.println("7. Удалить полку");
                System.out.println("8. Удалить книгу");
                System.out.println("9. Вывести шкафы в лексикографическом порядке");
                System.out.println("10. Вывести книги на указанном этаже");
                System.out.println("11. Вывести книги с разницей года издания и написания > 10 лет");
                System.out.println("12. Сбросить неключевые поля к значениям по умолчанию");
                System.out.println("0. Выход");
                System.out.print("Выберите действие: ");

                int choice;
                try {
                    choice = scanner.nextInt();
                } catch (InputMismatchException e) {
                    System.out.println("Ошибка: введите число.");
                    scanner.nextLine();
                    continue;
                }
                scanner.nextLine();

                switch (choice) {
                    case 1:
                        viewAllShelves();
                        break;
                    case 2:
                        viewAllBooks();
                        break;
                    case 3:
                        addShelf();
                        break;
                    case 4:
                        addBook();
                        break;
                    case 5:
                        updateShelf();
                        break;
                    case 6:
                        updateBook();
                        break;
                    case 7:
                        deleteShelf();
                        break;
                    case 8:
                        deleteBook();
                        break;
                    case 9:
                        printCabinets();
                        break;
                    case 10:
                        printBooksByFloor();
                        break;
                    case 11:
                        printBooksWithYearDifference();
                        break;
                    case 12:
                        resetNonKeyFieldsToDefault();
                        break;
                    case 0:
                        exit = true;
                        break;
                    default:
                        System.out.println("Неверный выбор. Попробуйте снова.");
                }
            }

        } catch (Exception e) {
            System.err.println("Ошибка: " + e.getMessage());
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.err.println("Ошибка при закрытии соединения: " + e.getMessage());
            }
        }
    }

    // Просмотр всех полок
    private static void viewAllShelves() throws SQLException {
        System.out.println("\nСписок всех полок:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM Shelves")) {

            System.out.printf("%-5s %-10s %-10s %-10s%n",
                    "№", "Этаж", "Шкаф", "Полка");
            System.out.println("------------------------------------");

            int counter = 1;
            while (rs.next()) {
                System.out.printf("%-5d %-10d %-10d %-10d%n",
                        counter++,
                        rs.getInt("floor"),
                        rs.getInt("cabinet"),
                        rs.getInt("shelf"));
            }
        }
    }

    // Просмотр всех книг
    private static void viewAllBooks() throws SQLException {
        System.out.println("\nСписок всех книг:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT b.*, s.floor, s.cabinet, s.shelf " +
                     "FROM Books b JOIN Shelves s ON b.shelf_id = s.shelf_id")) {

            System.out.printf("%-5s %-30s %-40s %-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s%n",
                    "№", "Автор", "Название", "Издательство", "Год изд.", "Страниц", "Год напис.", "Вес (г)", "Этаж", "Шкаф", "Полка");
            System.out.println("---------------------------------------------------------------------------------------------");

            int counter = 1;
            while (rs.next()) {
                System.out.printf("%-5d %-30s %-40s %-20s %-10d %-10d %-10d %-10d %-10d %-10d %-10d%n",
                        counter++,
                        rs.getString("author"),
                        rs.getString("title"),
                        rs.getString("publisher"),
                        rs.getInt("publication_year"),
                        rs.getInt("page_count"),
                        rs.getInt("writing_year"),
                        rs.getInt("weight_grams"),
                        rs.getInt("floor"),
                        rs.getInt("cabinet"),
                        rs.getInt("shelf"));
            }
        }
    }

    // Добавление новой полки
    private static void addShelf() throws SQLException {
        System.out.println("\nДобавление новой полки:");
        System.out.print("Этаж: ");
        int floor;
        try {
            floor = scanner.nextInt();
            if (floor < 0) {
                System.out.println("Ошибка: этаж не может быть отрицательным.");
                scanner.nextLine();
                return;
            }
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: этаж должен быть числом.");
            scanner.nextLine();
            return;
        }

        System.out.print("Шкаф (номер): ");
        int cabinet;
        try {
            cabinet = scanner.nextInt();
            if (cabinet < 0) {
                System.out.println("Ошибка: номер шкафа не может быть отрицательным.");
                scanner.nextLine();
                return;
            }
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: номер шкафа должен быть числом.");
            scanner.nextLine();
            return;
        }

        System.out.print("Полка (номер): ");
        int shelf;
        try {
            shelf = scanner.nextInt();
            if (shelf < 0) {
                System.out.println("Ошибка: номер полки не может быть отрицательным.");
                scanner.nextLine();
                return;
            }
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: номер полки должен быть числом.");
            scanner.nextLine();
            return;
        }
        scanner.nextLine();

        // Проверка на существование комбинации cabinet и shelf
        String checkSql = "SELECT COUNT(*) FROM Shelves WHERE cabinet = ? AND shelf = ?";
        try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
            checkStmt.setInt(1, cabinet);
            checkStmt.setInt(2, shelf);
            ResultSet rs = checkStmt.executeQuery();
            if (rs.next() && rs.getInt(1) > 0) {
                System.out.println("Ошибка: полка " + shelf + " в шкафу " + cabinet + " уже существует.");
                return;
            }
        }

        String sql = "INSERT INTO Shelves (floor, cabinet, shelf) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setInt(1, floor);
            pstmt.setInt(2, cabinet);
            pstmt.setInt(3, shelf);
            int rowsAffected = pstmt.executeUpdate();
            if (rowsAffected > 0) {
                try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        System.out.println("Полка успешно добавлена с ID: " + generatedKeys.getInt(1));
                    }
                }
            } else {
                System.out.println("Не удалось добавить полку.");
            }
        } catch (SQLException e) {
            if (e.getSQLState().equals("23505")) {
                System.out.println("Ошибка: полка " + shelf + " в шкафу " + cabinet + " уже существует.");
            } else {
                System.err.println("Ошибка при добавлении полки: " + e.getMessage());
                throw e;
            }
        }
    }

    // Добавление новой книги
    private static void addBook() throws SQLException {
        System.out.println("\nДобавление новой книги:");
        System.out.print("Автор: ");
        String author = scanner.nextLine();
        System.out.print("Название: ");
        String title = scanner.nextLine();
        System.out.print("Издательство: ");
        String publisher = scanner.nextLine();
        System.out.print("Год издания: ");
        int publicationYear;
        try {
            publicationYear = scanner.nextInt();
            if (publicationYear < 0) {
                System.out.println("Ошибка: год издания не может быть отрицательным.");
                scanner.nextLine();
                return;
            }
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: год издания должен быть числом.");
            scanner.nextLine();
            return;
        }
        System.out.print("Количество страниц: ");
        int pageCount;
        try {
            pageCount = scanner.nextInt();
            if (pageCount < 0) {
                System.out.println("Ошибка: количество страниц не может быть отрицательным.");
                scanner.nextLine();
                return;
            }
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: количество страниц должно быть числом.");
            scanner.nextLine();
            return;
        }
        System.out.print("Год написания: ");
        int writingYear;
        try {
            writingYear = scanner.nextInt();
            if (writingYear < 0) {
                System.out.println("Ошибка: год написания не может быть отрицательным.");
                scanner.nextLine();
                return;
            }
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: год написания должен быть числом.");
            scanner.nextLine();
            return;
        }
        System.out.print("Вес (граммы): ");
        int weightGrams;
        try {
            weightGrams = scanner.nextInt();
            if (weightGrams < 0) {
                System.out.println("Ошибка: вес не может быть отрицательным.");
                scanner.nextLine();
                return;
            }
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: вес должен быть числом.");
            scanner.nextLine();
            return;
        }
        scanner.nextLine();

        System.out.println("\nВыберите полку:");
        int shelfId = selectShelf();
        if (shelfId == -1) {
            System.out.println("Не удалось выбрать полку. Операция отменена.");
            return;
        }

        String sql = "INSERT INTO Books (author, title, publisher, publication_year, page_count, writing_year, weight_grams, shelf_id) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, author);
            pstmt.setString(2, title);
            pstmt.setString(3, publisher);
            pstmt.setInt(4, publicationYear);
            pstmt.setInt(5, pageCount);
            pstmt.setInt(6, writingYear);
            pstmt.setInt(7, weightGrams);
            pstmt.setInt(8, shelfId);
            int rowsAffected = pstmt.executeUpdate();
            if (rowsAffected > 0) {
                try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        System.out.println("Книга успешно добавлена с ID: " + generatedKeys.getInt(1));
                    }
                }
            } else {
                System.out.println("Не удалось добавить книгу.");
            }
        }
    }

    // Выбор полки
    private static int selectShelf() throws SQLException {
        System.out.println("\nСписок полок:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM Shelves ORDER BY floor, cabinet, shelf")) {

            System.out.printf("%-5s %-10s %-10s %-10s%n",
                    "ID", "Этаж", "Шкаф", "Полка");
            System.out.println("------------------------------------");

            List<Integer> shelfIds = new ArrayList<>();
            while (rs.next()) {
                shelfIds.add(rs.getInt("shelf_id"));
                System.out.printf("%-5d %-10d %-10d %-10d%n",
                        rs.getInt("shelf_id"),
                        rs.getInt("floor"),
                        rs.getInt("cabinet"),
                        rs.getInt("shelf"));
            }

            if (shelfIds.isEmpty()) {
                System.out.println("Полки отсутствуют. Сначала добавьте полку.");
                return -1;
            }

            System.out.print("\nВведите ID полки: ");
            try {
                int shelfId = scanner.nextInt();
                if (!shelfIds.contains(shelfId)) {
                    System.out.println("Неверный ID полки.");
                    return -1;
                }
                return shelfId;
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: введите число.");
                scanner.nextLine();
                return -1;
            }
        }
    }

    // Изменение полки
    private static void updateShelf() throws SQLException {
        conn.setAutoCommit(false);
        try {
            List<Integer> shelfIds = new ArrayList<>();
            System.out.println("\nСписок всех полок:");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM Shelves")) {

                System.out.printf("%-5s %-10s %-10s %-10s%n",
                        "№", "Этаж", "Шкаф", "Полка");
                System.out.println("------------------------------------");

                int counter = 1;
                while (rs.next()) {
                    shelfIds.add(rs.getInt("shelf_id"));
                    System.out.printf("%-5d %-10d %-10d %-10d%n",
                            counter++,
                            rs.getInt("floor"),
                            rs.getInt("cabinet"),
                            rs.getInt("shelf"));
                }
            }

            if (shelfIds.isEmpty()) {
                System.out.println("Нет полок для изменения.");
                conn.rollback();
                return;
            }

            System.out.print("\nВведите порядковый номер полки для изменения: ");
            int listNumber;
            try {
                listNumber = scanner.nextInt();
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: введите число.");
                scanner.nextLine();
                conn.rollback();
                return;
            }
            scanner.nextLine();

            if (listNumber < 1 || listNumber > shelfIds.size()) {
                System.out.println("Неверный номер полки.");
                conn.rollback();
                return;
            }

            int shelfId = shelfIds.get(listNumber - 1);

            System.out.print("Новый этаж (введите -1, чтобы не менять): ");
            int floor;
            try {
                floor = scanner.nextInt();
                if (floor < -1) {
                    System.out.println("Ошибка: этаж не может быть меньше -1.");
                    scanner.nextLine();
                    conn.rollback();
                    return;
                }
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: этаж должен быть числом.");
                scanner.nextLine();
                conn.rollback();
                return;
            }

            System.out.print("Новый шкаф (введите -1, чтобы не менять): ");
            int cabinet;
            try {
                cabinet = scanner.nextInt();
                if (cabinet < -1) {
                    System.out.println("Ошибка: номер шкафа не может быть меньше -1.");
                    scanner.nextLine();
                    conn.rollback();
                    return;
                }
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: номер шкафа должен быть числом.");
                scanner.nextLine();
                conn.rollback();
                return;
            }

            System.out.print("Новая полка (введите -1, чтобы не менять): ");
            int shelf;
            try {
                shelf = scanner.nextInt();
                if (shelf < -1) {
                    System.out.println("Ошибка: номер полки не может быть меньше -1.");
                    scanner.nextLine();
                    conn.rollback();
                    return;
                }
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: номер полки должен быть числом.");
                scanner.nextLine();
                conn.rollback();
                return;
            }
            scanner.nextLine();

            // Проверка на существование комбинации cabinet и shelf
            if (cabinet != -1 && shelf != -1) {
                String checkSql = "SELECT COUNT(*) FROM Shelves WHERE cabinet = ? AND shelf = ? AND shelf_id != ?";
                try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                    checkStmt.setInt(1, cabinet);
                    checkStmt.setInt(2, shelf);
                    checkStmt.setInt(3, shelfId);
                    ResultSet rs = checkStmt.executeQuery();
                    if (rs.next() && rs.getInt(1) > 0) {
                        System.out.println("Ошибка: полка " + shelf + " в шкафу " + cabinet + " уже существует.");
                        conn.rollback();
                        return;
                    }
                }
            }

            boolean hasUpdates = floor != -1 || cabinet != -1 || shelf != -1;
            if (!hasUpdates) {
                System.out.println("Не указано ни одного поля для обновления.");
                conn.rollback();
                return;
            }

            StringBuilder sql = new StringBuilder("UPDATE Shelves SET ");
            List<Object> params = new ArrayList<>();

            if (floor != -1) {
                sql.append("floor = ?, ");
                params.add(floor);
            }
            if (cabinet != -1) {
                sql.append("cabinet = ?, ");
                params.add(cabinet);
            }
            if (shelf != -1) {
                sql.append("shelf = ?, ");
                params.add(shelf);
            }

            sql.delete(sql.length() - 2, sql.length());
            sql.append(" WHERE shelf_id = ?");
            params.add(shelfId);

            try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setInt(i + 1, (Integer) params.get(i));
                }

                int rowsAffected = pstmt.executeUpdate();
                if (rowsAffected > 0) {
                    System.out.println("Полка успешно обновлена.");
                    conn.commit();
                } else {
                    System.out.println("Полка не найдена.");
                    conn.rollback();
                }
            } catch (SQLException e) {
                if (e.getSQLState().equals("23505")) {
                    System.out.println("Ошибка: полка " + shelf + " в шкафу " + cabinet + " уже существует.");
                    conn.rollback();
                } else {
                    System.err.println("Ошибка при обновлении полки: " + e.getMessage());
                    conn.rollback();
                    throw e;
                }
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // Изменение книги
    private static void updateBook() throws SQLException {
        conn.setAutoCommit(false);
        try {
            List<Integer> bookIds = new ArrayList<>();
            System.out.println("\nСписок всех книг:");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT b.*, s.floor, s.cabinet, s.shelf " +
                         "FROM Books b JOIN Shelves s ON b.shelf_id = s.shelf_id")) {

                System.out.printf("%-5s %-30s %-40s %-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s%n",
                        "№", "Автор", "Название", "Издательство", "Год изд.", "Страниц", "Год напис.", "Вес (г)", "Этаж", "Шкаф", "Полка");
                System.out.println("---------------------------------------------------------------------------------------------");

                int counter = 1;
                while (rs.next()) {
                    bookIds.add(rs.getInt("book_id"));
                    System.out.printf("%-5d %-30s %-40s %-20s %-10d %-10d %-10d %-10d %-10d %-10d %-10d%n",
                            counter++,
                            rs.getString("author"),
                            rs.getString("title"),
                            rs.getString("publisher"),
                            rs.getInt("publication_year"),
                            rs.getInt("page_count"),
                            rs.getInt("writing_year"),
                            rs.getInt("weight_grams"),
                            rs.getInt("floor"),
                            rs.getInt("cabinet"),
                            rs.getInt("shelf"));
                }
            }

            if (bookIds.isEmpty()) {
                System.out.println("Нет книг для изменения.");
                conn.rollback();
                return;
            }

            System.out.print("\nВведите порядковый номер книги для изменения: ");
            int listNumber;
            try {
                listNumber = scanner.nextInt();
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: введите число.");
                scanner.nextLine();
                conn.rollback();
                return;
            }
            scanner.nextLine();

            if (listNumber < 1 || listNumber > bookIds.size()) {
                System.out.println("Неверный номер книги.");
                conn.rollback();
                return;
            }

            int bookId = bookIds.get(listNumber - 1);

            System.out.print("Новый автор (оставьте пустым, чтобы не менять): ");
            String author = scanner.nextLine();
            System.out.print("Новое название (оставьте пустым, чтобы не менять): ");
            String title = scanner.nextLine();
            System.out.print("Новое издательство (оставьте пустым, чтобы не менять): ");
            String publisher = scanner.nextLine();
            System.out.print("Новый год издания (введите -1, чтобы не менять): ");
            int publicationYear;
            try {
                publicationYear = scanner.nextInt();
                if (publicationYear < -1) {
                    System.out.println("Ошибка: год издания не может быть меньше -1.");
                    scanner.nextLine();
                    conn.rollback();
                    return;
                }
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: год издания должен быть числом.");
                scanner.nextLine();
                conn.rollback();
                return;
            }
            System.out.print("Новое количество страниц (введите -1, чтобы не менять): ");
            int pageCount;
            try {
                pageCount = scanner.nextInt();
                if (pageCount < -1) {
                    System.out.println("Ошибка: количество страниц не может быть меньше -1.");
                    scanner.nextLine();
                    conn.rollback();
                    return;
                }
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: количество страниц должно быть числом.");
                scanner.nextLine();
                conn.rollback();
                return;
            }
            System.out.print("Новый год написания (введите -1, чтобы не менять): ");
            int writingYear;
            try {
                writingYear = scanner.nextInt();
                if (writingYear < -1) {
                    System.out.println("Ошибка: год написания не может быть меньше -1.");
                    scanner.nextLine();
                    conn.rollback();
                    return;
                }
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: год написания должен быть числом.");
                scanner.nextLine();
                conn.rollback();
                return;
            }
            System.out.print("Новый вес (граммы, введите -1, чтобы не менять): ");
            int weightGrams;
            try {
                weightGrams = scanner.nextInt();
                if (weightGrams < -1) {
                    System.out.println("Ошибка: вес не может быть меньше -1.");
                    scanner.nextLine();
                    conn.rollback();
                    return;
                }
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: вес должен быть числом.");
                scanner.nextLine();
                conn.rollback();
                return;
            }
            scanner.nextLine();

            System.out.println("\nВыберите действие для полки:");
            System.out.println("1. Оставить текущую");
            System.out.println("2. Изменить полку");
            System.out.print("Ваш выбор: ");
            int shelfChoice;
            try {
                shelfChoice = scanner.nextInt();
            } catch (InputMismatchException e) {
                System.out.println("Ошибка: введите число.");
                scanner.nextLine();
                conn.rollback();
                return;
            }
            scanner.nextLine();

            Integer newShelfId = null;
            if (shelfChoice == 2) {
                System.out.println("\nВыберите полку:");
                newShelfId = selectShelf();
                if (newShelfId == -1) {
                    System.out.println("Не удалось выбрать полку. Полка не изменена.");
                    newShelfId = null;
                }
            } else if (shelfChoice != 1) {
                System.out.println("Неверный выбор. Полка не изменена.");
            }

            boolean hasUpdates = !author.isEmpty() || !title.isEmpty() || !publisher.isEmpty() ||
                    publicationYear != -1 || pageCount != -1 || writingYear != -1 || weightGrams != -1 || newShelfId != null;

            if (!hasUpdates) {
                System.out.println("Не указано ни одного поля для обновления.");
                conn.rollback();
                return;
            }

            StringBuilder sql = new StringBuilder("UPDATE Books SET ");
            List<Object> params = new ArrayList<>();

            if (!author.isEmpty()) {
                sql.append("author = ?, ");
                params.add(author);
            }
            if (!title.isEmpty()) {
                sql.append("title = ?, ");
                params.add(title);
            }
            if (!publisher.isEmpty()) {
                sql.append("publisher = ?, ");
                params.add(publisher);
            }
            if (publicationYear != -1) {
                sql.append("publication_year = ?, ");
                params.add(publicationYear);
            }
            if (pageCount != -1) {
                sql.append("page_count = ?, ");
                params.add(pageCount);
            }
            if (writingYear != -1) {
                sql.append("writing_year = ?, ");
                params.add(writingYear);
            }
            if (weightGrams != -1) {
                sql.append("weight_grams = ?, ");
                params.add(weightGrams);
            }
            if (newShelfId != null) {
                sql.append("shelf_id = ?, ");
                params.add(newShelfId);
            }

            sql.delete(sql.length() - 2, sql.length());
            sql.append(" WHERE book_id = ?");
            params.add(bookId);

            try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                for (int i = 0; i < params.size(); i++) {
                    Object param = params.get(i);
                    if (param instanceof String) {
                        pstmt.setString(i + 1, (String) param);
                    } else if (param instanceof Integer) {
                        pstmt.setInt(i + 1, (Integer) param);
                    }
                }

                int rowsAffected = pstmt.executeUpdate();
                if (rowsAffected > 0) {
                    System.out.println("Книга успешно обновлена.");
                    conn.commit();
                } else {
                    System.out.println("Книга не найдена.");
                    conn.rollback();
                }
            }
        } catch (SQLException e) {
            conn.rollback();
            System.err.println("Ошибка при обновлении книги: " + e.getMessage());
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // Удаление полки
    private static void deleteShelf() throws SQLException {
        List<Integer> shelfIds = new ArrayList<>();
        System.out.println("\nСписок всех полок:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM Shelves")) {

            System.out.printf("%-5s %-10s %-10s %-10s%n",
                    "№", "Этаж", "Шкаф", "Полка");
            System.out.println("------------------------------------");

            int counter = 1;
            while (rs.next()) {
                shelfIds.add(rs.getInt("shelf_id"));
                System.out.printf("%-5d %-10d %-10d %-10d%n",
                        counter++,
                        rs.getInt("floor"),
                        rs.getInt("cabinet"),
                        rs.getInt("shelf"));
            }
        }

        if (shelfIds.isEmpty()) {
            System.out.println("Нет полок для удаления.");
            return;
        }

        System.out.print("\nВведите порядковый номер полки для удаления: ");
        int listNumber;
        try {
            listNumber = scanner.nextInt();
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: введите число.");
            scanner.nextLine();
            return;
        }
        scanner.nextLine();

        if (listNumber < 1 || listNumber > shelfIds.size()) {
            System.out.println("Неверный номер полки.");
            return;
        }

        int shelfId = shelfIds.get(listNumber - 1);

        conn.setAutoCommit(false);

        try {
            String sql = "DELETE FROM Shelves WHERE shelf_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, shelfId);
                int rowsAffected = pstmt.executeUpdate();

                if (rowsAffected > 0) {
                    System.out.println("Полка и связанные книги успешно удалены.");
                    conn.commit();
                } else {
                    System.out.println("Полка не найдена.");
                    conn.rollback();
                }
            }
        } catch (SQLException e) {
            conn.rollback();
            System.err.println("Ошибка при удалении полки: " + e.getMessage());
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // Удаление книги
    private static void deleteBook() throws SQLException {
        List<Integer> bookIds = new ArrayList<>();
        System.out.println("\nСписок всех книг:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT b.*, s.floor, s.cabinet, s.shelf " +
                     "FROM Books b JOIN Shelves s ON b.shelf_id = s.shelf_id")) {

            System.out.printf("%-5s %-30s %-40s %-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s%n",
                    "№", "Автор", "Название", "Издательство", "Год изд.", "Страниц", "Год напис.", "Вес (г)", "Этаж", "Шкаф", "Полка");
            System.out.println("---------------------------------------------------------------------------------------------");

            int counter = 1;
            while (rs.next()) {
                bookIds.add(rs.getInt("book_id"));
                System.out.printf("%-5d %-30s %-40s %-20s %-10d %-10d %-10d %-10d %-10d %-10d %-10d%n",
                        counter++,
                        rs.getString("author"),
                        rs.getString("title"),
                        rs.getString("publisher"),
                        rs.getInt("publication_year"),
                        rs.getInt("page_count"),
                        rs.getInt("writing_year"),
                        rs.getInt("weight_grams"),
                        rs.getInt("floor"),
                        rs.getInt("cabinet"),
                        rs.getInt("shelf"));
            }
        }

        if (bookIds.isEmpty()) {
            System.out.println("Нет книг для удаления.");
            return;
        }

        System.out.print("\nВведите порядковый номер книги для удаления: ");
        int listNumber;
        try {
            listNumber = scanner.nextInt();
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: введите число.");
            scanner.nextLine();
            return;
        }
        scanner.nextLine();

        if (listNumber < 1 || listNumber > bookIds.size()) {
            System.out.println("Неверный номер книги.");
            return;
        }

        int bookId = bookIds.get(listNumber - 1);

        conn.setAutoCommit(false);

        try {
            String sql = "DELETE FROM Books WHERE book_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, bookId);
                int rowsAffected = pstmt.executeUpdate();

                if (rowsAffected > 0) {
                    System.out.println("Книга успешно удалена.");
                    conn.commit();
                } else {
                    System.out.println("Книга не найдена.");
                    conn.rollback();
                }
            }
        } catch (SQLException e) {
            conn.rollback();
            System.err.println("Ошибка при удалении книги: " + e.getMessage());
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // Вывод шкафов
    private static void printCabinets() throws SQLException {
        System.out.println("\nШкафы в лексикографическом порядке:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT DISTINCT cabinet FROM Shelves ORDER BY cabinet ASC")) {

            System.out.printf("%-10s%n", "Шкаф");
            System.out.println("----------");

            while (rs.next()) {
                System.out.printf("%-10d%n", rs.getInt("cabinet"));
            }
        }
    }

    // Вывод книг на определенном этаже
    private static void printBooksByFloor() throws SQLException {
        System.out.print("\nВведите этаж: ");
        int floor;
        try {
            floor = scanner.nextInt();
            if (floor < 0) {
                System.out.println("Ошибка: этаж не может быть отрицательным.");
                scanner.nextLine();
                return;
            }
        } catch (InputMismatchException e) {
            System.out.println("Ошибка: этаж должен быть числом.");
            scanner.nextLine();
            return;
        }
        scanner.nextLine();

        System.out.println("\nКниги на этаже " + floor + " в лексикографическом порядке:");
        String sql = "SELECT b.title, b.author " +
                "FROM Books b JOIN Shelves s ON b.shelf_id = s.shelf_id " +
                "WHERE s.floor = ? ORDER BY b.title ASC";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, floor);
            ResultSet rs = pstmt.executeQuery();

            System.out.printf("%-40s %-30s%n", "Название", "Автор");
            System.out.println("------------------------------------------------------------");

            boolean hasResults = false;
            while (rs.next()) {
                hasResults = true;
                System.out.printf("%-40s %-30s%n",
                        rs.getString("title"),
                        rs.getString("author"));
            }
            if (!hasResults) {
                System.out.println("Книги на указанном этаже отсутствуют.");
            }
        }
    }

    // Вывод книг с разницей года издания и написания более 10 лет
    private static void printBooksWithYearDifference() throws SQLException {
        System.out.println("\nКниги с разницей года издания и написания более 10 лет:");
        String sql = "SELECT title, author, publication_year, writing_year " +
                "FROM Books WHERE ABS(publication_year - writing_year) > 10";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.printf("%-40s %-30s %-15s %-15s%n",
                    "Название", "Автор", "Год издания", "Год написания");
            System.out.println("------------------------------------------------------------");

            boolean hasResults = false;
            while (rs.next()) {
                hasResults = true;
                System.out.printf("%-40s %-30s %-15d %-15d%n",
                        rs.getString("title"),
                        rs.getString("author"),
                        rs.getInt("publication_year"),
                        rs.getInt("writing_year"));
            }
            if (!hasResults) {
                System.out.println("Книги с разницей более 10 лет отсутствуют.");
            }
        }
    }

    // Сброс неключевых полей к значениям по умолчанию
    private static void resetNonKeyFieldsToDefault() throws SQLException {
        System.out.println("\nСброс неключевых полей к значениям по умолчанию...");
        conn.setAutoCommit(false);

        try {
            String sql1 = "UPDATE Shelves SET floor = ?, cabinet = ?, shelf = ?";
            try (PreparedStatement pstmt1 = conn.prepareStatement(sql1)) {
                pstmt1.setInt(1, DEFAULT_FLOOR);
                pstmt1.setInt(2, DEFAULT_CABINET);
                pstmt1.setInt(3, DEFAULT_SHELF);
                int rowsAffected = pstmt1.executeUpdate();
                System.out.println("Обновлено " + rowsAffected + " записей в таблице полок.");
            }

            String sql2 = "UPDATE Books SET author = ?, title = ?, publisher = ?, " +
                    "publication_year = ?, page_count = ?, writing_year = ?, weight_grams = ?";
            try (PreparedStatement pstmt2 = conn.prepareStatement(sql2)) {
                pstmt2.setString(1, DEFAULT_AUTHOR);
                pstmt2.setString(2, DEFAULT_TITLE);
                pstmt2.setString(3, DEFAULT_PUBLISHER);
                pstmt2.setInt(4, DEFAULT_PUBLICATION_YEAR);
                pstmt2.setInt(5, DEFAULT_PAGE_COUNT);
                pstmt2.setInt(6, DEFAULT_WRITING_YEAR);
                pstmt2.setInt(7, DEFAULT_WEIGHT_GRAMS);
                int rowsAffected = pstmt2.executeUpdate();
                System.out.println("Обновлено " + rowsAffected + " записей в таблице книг.");
            }

            conn.commit();
            System.out.println("Сброс значений по умолчанию успешно выполнен.");
        } catch (SQLException e) {
            conn.rollback();
            System.out.println("Ошибка при сбросе значений: " + e.getMessage());
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }
}