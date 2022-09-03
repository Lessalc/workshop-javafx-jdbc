package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao{
	
	Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {
		PreparedStatement st = null;
		
		try {
			st = conn.prepareStatement("INSERT INTO department "
					+ "(Name) VALUES (?);", Statement.RETURN_GENERATED_KEYS);
			
			st.setString(1, obj.getName());
			
			int rows = st.executeUpdate();
			
			if(rows > 0) {
				System.out.println("Department created successfully! Id= " + rows);
				obj.setId(rows);
			} else {
				throw new DbException("Error while creating Department");
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DB.closeStatement(st);
		}
		
	}

	@Override
	public void update(Department obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("UPDATE department "
					+ "SET Name= ? "
					+ "WHERE id = ?;", Statement.RETURN_GENERATED_KEYS);
			
			st.setString(1, obj.getName());
			st.setInt(2, obj.getId());
			
			int rows = st.executeUpdate();
			
			if(rows > 0) {
				System.out.println("Department with Id= "+obj.getId()+" updated successfully!");
			} else {
				throw new DbException("Error while updationg Department");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("DELETE FROM department "
					+ "WHERE Id= ?;", Statement.RETURN_GENERATED_KEYS);
			
			st.setInt(1, id);
			
			int rows = st.executeUpdate();
			
			if(rows > 0) {
				System.out.println("Department with Id= "+id+" deleted successfully!");
			} else {
				throw new DbException("No object found");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DB.closeStatement(st);
		}
		
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st= conn.prepareStatement("SELECT department.* "
					+ "FROM department "
					+ "WHERE department.id = ?;");
			
			st.setInt(1, id);
			rs = st.executeQuery();
			
			if(rs.next()) {
				
				return instantiateDepartment(rs);
				
			} else {
				return null;
			}
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("Id"));
		dep.setName(rs.getString("Name"));
		return dep;
	}

	@Override
	public List<Department> findAll() {
		List<Department> departments = new ArrayList<>();
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st= conn.prepareStatement("SELECT department.* "
					+ "FROM department ");
			
			rs = st.executeQuery();
			
			while(rs.next()) {
				departments.add(instantiateDepartment(rs));
			} 
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
		return departments;
	}

}
