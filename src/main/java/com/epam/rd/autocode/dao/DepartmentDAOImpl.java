package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DepartmentDAOImpl implements DepartmentDao {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final byte PARAMETER_ONE = 1;
    private static final byte PARAMETER_TWO = 2;
    private static final byte PARAMETER_THREE = 3;
    private static final byte COLUMN_ID = 1;
    private static final byte COLUMN_NAME = 2;
    private static final byte COLUMN_LOCATION = 3;
    private static final String GET_DEPARTMENT_BY_ID = "SELECT id, name, location FROM department WHERE id = ?";
    private static final String GET_ALL_DEPARTMENTS = "SELECT * FROM DEPARTMENT";
    private static final String SAVE = "INSERT INTO department VALUES ?, ?, ?";
    private static final String DELETE = "DELETE FROM department WHERE id = ?";

    @Override
    public Optional<Department> getById(BigInteger id) {
        Optional<Department> dep = Optional.empty();
        ResultSet resultSet = null;
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(GET_DEPARTMENT_BY_ID)) {
            preparedStatement.setString(PARAMETER_ONE, String.valueOf(id));
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                dep = Optional.of(new Department(
                    resultSet.getBigDecimal(COLUMN_ID).toBigInteger(),
                    resultSet.getString(COLUMN_NAME),
                    resultSet.getString(COLUMN_LOCATION)));
            }
        } catch (SQLException e) {
            LOGGER.error(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    LOGGER.error(e);
                }
            }
        }
        return dep;
    }

    @Override
    public List<Department> getAll() {
        List<Department> departmentList = new ArrayList<>();
        try (Connection connection = ConnectionSource.instance().createConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_ALL_DEPARTMENTS)) {

            while (resultSet.next()) {
                departmentList.add(new Department(
                    resultSet.getBigDecimal(COLUMN_ID).toBigInteger(),
                    resultSet.getString(COLUMN_NAME),
                    resultSet.getString(COLUMN_LOCATION)));
            }
        } catch (SQLException e) {
            LOGGER.error(e);
        }
        return departmentList;
    }

    @Override
    public Department save(Department department) {
        BigInteger id;
        if (!getById(department.getId()).equals(Optional.empty())) {
            delete(department);
        }
        id = department.getId();

        final String name = department.getName();
        final String location = department.getLocation();
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(SAVE)) {
            preparedStatement.setInt(PARAMETER_ONE, Integer.parseInt(String.valueOf(id)));
            preparedStatement.setString(PARAMETER_TWO, name);
            preparedStatement.setString(PARAMETER_THREE, location);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error(e);
        }
        return department;
    }

    @Override
    public void delete(Department department) {
        try (Connection connection = ConnectionSource.instance().createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE)) {
            preparedStatement.setString(PARAMETER_ONE, String.valueOf(department.getId()));
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error(e);
        }
    }
}
