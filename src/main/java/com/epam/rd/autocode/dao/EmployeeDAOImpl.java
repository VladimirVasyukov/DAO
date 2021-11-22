package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class EmployeeDAOImpl implements EmployeeDao {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final byte PARAMETER_ONE = 1;
    private static final byte PARAMETER_TWO = 2;
    private static final byte PARAMETER_THREE = 3;
    private static final byte PARAMETER_FOUR = 4;
    private static final byte PARAMETER_FIVE = 5;
    private static final byte PARAMETER_SIX = 6;
    private static final byte PARAMETER_SEVEN = 7;
    private static final byte PARAMETER_EIGHT = 8;
    private static final byte PARAMETER_NINE = 9;
    private static final byte COLUMN_ID = 1;
    private static final byte COLUMN_FIRST_NAME = 2;
    private static final byte COLUMN_LAST_NAME = 3;
    private static final byte COLUMN_MIDDLE_NAME = 4;
    private static final byte COLUMN_POSITION = 5;
    private static final byte COLUMN_MANAGER = 6;
    private static final byte COLUMN_HIREDATE = 7;
    private static final byte COLUMN_SALARY = 8;
    private static final byte COLUMN_DEPARTMENT = 9;
    private static final String GET_BY_DEPARTMENT =
        "SELECT id, firstname, lastname, middlename, position, manager, hiredate," +
            " salary, department FROM EMPLOYEE WHERE department = ?";
    private static final String GET_BY_MANAGER =
        "SELECT id, firstname, lastname, middlename, position, manager, hiredate, salary," +
            " department FROM EMPLOYEE WHERE manager = ?";
    private static final String GET_BY_ID =
        "SELECT id, firstname, lastname, middlename, position, manager, hiredate, salary, department " +
            "FROM EMPLOYEE WHERE id = ?";
    private static final String GET_ALL = "SELECT * FROM EMPLOYEE";
    private static final String SAVE = "INSERT INTO employee VALUES ?, ?, ?, ?, ?, ?, ?, ?, ?";
    private static final String DELETE = "DELETE FROM employee WHERE id = ?";

    @Override
    public List<Employee> getByDepartment(Department department) {
        List<Employee> employeeList = new ArrayList<>();
        ResultSet resultSet = null;
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(GET_BY_DEPARTMENT)) {
            preparedStatement.setString(PARAMETER_ONE, String.valueOf(department.getId()));
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                getDataFromResultSet(employeeList, resultSet);
            }
        } catch (SQLException e) {
            LOGGER.error(e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e);
            }
        }
        return employeeList;
    }

    @Override
    public List<Employee> getByManager(Employee employee) {
        List<Employee> employeeList = new ArrayList<>();
        ResultSet resultSet = null;
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(GET_BY_MANAGER)) {
            preparedStatement.setString(PARAMETER_ONE, String.valueOf(employee.getId()));
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                getDataFromResultSet(employeeList, resultSet);
            }
        } catch (SQLException e) {
            LOGGER.error(e);
        } finally {
            try {
                Objects.requireNonNull(resultSet).close();
            } catch (SQLException e) {
                LOGGER.error(e);
            }
        }
        return employeeList;
    }

    private void getDataFromResultSet(List<Employee> employeeList, ResultSet resultSet) throws SQLException {
        BigInteger id = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_ID)));
        FullName fullName = new FullName(
            resultSet.getString(COLUMN_FIRST_NAME),
            resultSet.getString(COLUMN_LAST_NAME),
            resultSet.getString(COLUMN_MIDDLE_NAME));
        Position position = Position.valueOf(resultSet.getString(COLUMN_POSITION));
        BigInteger managerId = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_MANAGER)));
        LocalDate hired = resultSet.getDate(COLUMN_HIREDATE).toLocalDate();
        BigDecimal salary = resultSet.getBigDecimal(COLUMN_SALARY);
        BigInteger departmentId = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_DEPARTMENT)));
        employeeList.add(new Employee(id, fullName, position, hired, salary, managerId, departmentId));
    }

    @Override
    public Optional<Employee> getById(BigInteger id) {
        Optional<Employee> employee = Optional.empty();
        ResultSet resultSet = null;
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(GET_BY_ID)) {
            preparedStatement.setString(PARAMETER_ONE, String.valueOf(id));
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                employee = Optional.of(new Employee(
                    new BigInteger(String.valueOf(resultSet.getLong(COLUMN_ID))),
                    new FullName(
                        resultSet.getString(COLUMN_FIRST_NAME),
                        resultSet.getString(COLUMN_LAST_NAME),
                        resultSet.getString(COLUMN_MIDDLE_NAME)),
                    Position.valueOf(resultSet.getString(COLUMN_POSITION)),
                    resultSet.getDate(COLUMN_HIREDATE).toLocalDate(),
                    resultSet.getBigDecimal(COLUMN_SALARY),
                    new BigInteger(String.valueOf(resultSet.getLong(COLUMN_MANAGER))),
                    new BigInteger(String.valueOf(resultSet.getLong(COLUMN_DEPARTMENT)))));
            }
        } catch (SQLException e) {
            LOGGER.error(e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e);
            }
        }
        return employee;
    }

    @Override
    public List<Employee> getAll() {
        List<Employee> employeeList = new ArrayList<>();
        try (Connection connection = ConnectionSource.instance().createConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_ALL)) {
            while (resultSet.next()) {
                getDataFromResultSet(employeeList, resultSet);
            }
        } catch (SQLException e) {
            LOGGER.error(e);
        }
        return employeeList;
    }


    @Override
    public Employee save(Employee employee) {
        BigInteger id;
        if (!getById(employee.getId()).equals(Optional.empty())) {
            delete(employee);
        }
        id = employee.getId();

        final FullName fullName = employee.getFullName();
        final Position position = employee.getPosition();
        final LocalDate localDate = employee.getHired();
        final BigDecimal salary = employee.getSalary();
        final BigInteger managerId = employee.getManagerId();
        final BigInteger departmentId = employee.getDepartmentId();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(SAVE)) {
            preparedStatement.setInt(PARAMETER_ONE, Integer.parseInt(String.valueOf(id)));
            preparedStatement.setString(PARAMETER_TWO, fullName.getFirstName());
            preparedStatement.setString(PARAMETER_THREE, fullName.getLastName());
            preparedStatement.setString(PARAMETER_FOUR, fullName.getMiddleName());
            preparedStatement.setString(PARAMETER_FIVE, position.toString());
            preparedStatement.setInt(PARAMETER_SIX, Integer.parseInt(String.valueOf(managerId)));
            preparedStatement.setString(PARAMETER_SEVEN, localDate.toString());
            preparedStatement.setInt(PARAMETER_EIGHT, salary.intValue());
            preparedStatement.setInt(PARAMETER_NINE, Integer.parseInt(String.valueOf(departmentId)));
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error(e);
        }
        return employee;
    }


    @Override
    public void delete(Employee employee) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE)) {
            preparedStatement.setString(PARAMETER_ONE, String.valueOf(employee.getId()));
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error(e);
        }
    }
}
