package com.wrox.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

public class DbTestServlet extends HttpServlet {
	
	private static final String JOLT_TABLE = "JoltData";
	
	public void doGet(HttpServletRequest req, HttpServletResponse res)
	throws IOException, ServletException {
		res.setContentType("text/plain");
		PrintWriter out = res.getWriter();
		
		DataSource ds = null;
		try {
			InitialContext ctx = new InitialContext();
			ds = (DataSource) ctx.lookup("java:/DefaultDS");
		} catch (NamingException e) {
			out.println(e);
			return;
		}
		
		Connection connection = null;
		try {
			connection = ds.getConnection();
			
			createAndPopulateTableIfNecessary(connection);
			out.println("tables ready at " + new java.util.Date());
			
			printAllRows(connection, out);
		} catch (Exception e){
			out.println(e);
		} finally {
			try {
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				out.println("could not close connection");
			}
		}
	}
	
	protected void createAndPopulateTableIfNecessary(Connection connection)
	throws SQLException
	{
		
		if (! existsTable(connection, JOLT_TABLE)) {
			String[] statements = {
					"create table " + JOLT_TABLE + "("+
					"programmer varchar (32),"+
					"day varchar (3),"+
					"cups integer);",
					"insert into JoltData values ('Gilbert', 'Mon', 1);",
					"insert into JoltData values ('Wally', 'Mon', 2);",
					"insert into JoltData values ('Edgar', 'Tue', 8);",
					"insert into JoltData values ('Wally', 'Tue', 2);",
					"insert into JoltData values ('Eugene', 'Tue', 3);",
					"insert into JoltData values ('Josephine', 'Wed', 2);",
					"insert into JoltData values ('Eugene', 'Thu', 3);",
					"insert into JoltData values ('Gilbert', 'Thu', 1);",
					"insert into JoltData values ('Clarence', 'Fri', 9);",
					"insert into JoltData values ('Edgar', 'Fri', 3);",
					"insert into JoltData values ('Josephine', 'Fri', 4);",
			};
			Statement stmt = connection.createStatement();
			for (int i=0; i< statements.length; i++) {
				stmt.execute(statements[i]);
			}
			stmt.close();
		}
	}
	
	protected boolean existsTable(Connection connection, String tableName)
	throws SQLException
	{
		ResultSet rs = connection.getMetaData().getTables(null, null, "%", null);
		while (rs.next()) {
			if (tableName.equalsIgnoreCase(rs.getString("TABLE_NAME"))) {
				return true;
			}
		}
		return false;
	}
	
	protected void printAllRows(Connection connection, PrintWriter out)
	throws SQLException
	{
		Statement stmt = connection.createStatement();
		ResultSet result = stmt.executeQuery("select * from " + JOLT_TABLE);
		out.println();
		out.println("programmer day cups");
		while (result.next()) {
			out.print(result.getString("programmer") + " ");
			out.print(result.getString("day") + " ");
			out.println(result.getString("cups"));
		}
		stmt.close();
	}
}
