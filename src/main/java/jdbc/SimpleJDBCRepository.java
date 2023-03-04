package jdbc;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SimpleJDBCRepository {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private Statement st = null;

    private static final String CREATE_USER_SQL = "insert into myusers(firstname, lastname, age) values (?, ?, ?);";
    private static final String UPDATE_USER_SQL = "UPDATE myusers SET firstname = ?, lastname = ?, age = ? WHERE id = ?";
    private static final String DELETE_USER = "DELETE FROM myusers WHERE id = ?";
    private static final String FIND_USER_BY_ID_SQL = "SELECT * FROM myusers WHERE id = ?";
    private static final String FIND_USER_BY_NAME_SQL = "SELECT * FROM myusers WHERE firstname = ?";
    private static final String FIND_ALL_USERS_SQL = "SELECT * FROM myusers";

    public Long createUser(User user) {
        Long id = null;
        try (Connection con = CustomDataSource.getInstance().getConnection();
             PreparedStatement statement = con.prepareStatement(CREATE_USER_SQL, Statement.RETURN_GENERATED_KEYS)) {
            statement.setObject(1, user.getFirstName());
            statement.setObject(2, user.getLastName());
            statement.setObject(3, user.getAge());
            statement.execute();
            ResultSet generatedKeys = statement.getGeneratedKeys();
            if (generatedKeys.next()) {
                id = generatedKeys.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return id;
    }

    public User findUserById(Long userId) {
        try(Connection con = CustomDataSource.getInstance().getConnection();
            PreparedStatement prepareStatement = con.prepareStatement(FIND_USER_BY_ID_SQL)) {
            User user = new User();
            prepareStatement.setLong(1, userId);
            ResultSet resultSet = prepareStatement.executeQuery();
            if(resultSet.next()) {
                user = build(resultSet);
            }
            return user;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public User findUserByName(String userName) {
        try(Connection con = CustomDataSource.getInstance().getConnection();
            PreparedStatement prepareStatement = con.prepareStatement(FIND_USER_BY_NAME_SQL)) {
            User user = new User();
            prepareStatement.setString(1, userName);
            ResultSet resultSet = prepareStatement.executeQuery();
            if(resultSet.next()) {
                user = build(resultSet);
            }
            return user;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public List<User> findAllUser() {
        List<User> users = new ArrayList<>();
        try(Connection con = CustomDataSource.getInstance().getConnection();
            Statement statement = con.createStatement()) {
            ResultSet resultSet = statement.executeQuery(FIND_ALL_USERS_SQL);
            while (resultSet.next()) {
                users.add(build(resultSet));
            }
            return users;
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public User updateUser(User user) {
        try(Connection con = CustomDataSource.getInstance().getConnection();
            PreparedStatement prepareStatement = con.prepareStatement(UPDATE_USER_SQL)) {
            prepareStatement.setString(1, user.getFirstName());
            prepareStatement.setString(2, user.getLastName());
            prepareStatement.setInt(3, user.getAge());
            prepareStatement.setLong(4, user.getId());
            if(prepareStatement.executeUpdate() != 0) {
                return findUserById(user.getId());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
        return new User();
    }

    public void deleteUser(Long userId) {
        try (Connection con = CustomDataSource.getInstance().getConnection();
             PreparedStatement prepareStatement = con.prepareStatement(DELETE_USER)) {
            prepareStatement.setLong(1, userId);
            prepareStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private User build(ResultSet resultSet) throws SQLException {
        return User.builder()
                .id(resultSet.getLong("id"))
                .firstName(resultSet.getString("firstname"))
                .lastName(resultSet.getString("lastname"))
                .age(resultSet.getInt("age"))
                .build();
    }
}
