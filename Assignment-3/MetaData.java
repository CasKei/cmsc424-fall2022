import java.sql.*;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;

public class MetaData {
	static String dataTypeName(int i) {
		switch (i) {
			case java.sql.Types.INTEGER:
				return "Integer";
			case java.sql.Types.REAL:
				return "Real";
			case java.sql.Types.VARCHAR:
				return "Varchar";
			case java.sql.Types.TIMESTAMP:
				return "Timestamp";
			case java.sql.Types.DATE:
				return "Date";
		}
		return "Other";
	}

	public static void executeMetadata(String databaseName) {
		/*************
		 * Add you code to connect to the database and print out the metadta for the
		 * database databaseName.
		 ************/
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			// System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in
			// your library path!");
			e.printStackTrace();
			return;
		}

		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + databaseName, "root",
					"root");
		} catch (SQLException e) {
			// System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		if (connection == null) {
			return;
		}

		Statement stmt = null;
		String query = "select table_name from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE'";
		HashSet<String> tables = new HashSet<String>();
		try {
			stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				tables.add(rs.getString("table_name"));
			}
			System.out.println("### Tables in the Database");
			for (String table : tables) {
				System.out.println("-- Table " + table.toUpperCase());

				// Attr

				DatabaseMetaData dbmd = connection.getMetaData();
				ResultSet rs1 = dbmd.getColumns(null, null, table, null);
				ArrayList<String> attr = new ArrayList<String>();
				while (rs1.next()) {
					String columnType = rs1.getString("DATA_TYPE");
					String columnName = rs1.getString("COLUMN_NAME").toUpperCase();

					if (columnType.equals("2")) {
						columnType = "Numeric";
					} else if (columnType.equals("4")) {
						columnType = "Integer";
					} else if (columnType.equals("12")) {
						columnType = "Varchar";
					} else if (columnType.equals("91")) {
						columnType = "Date";
					}
					attr.add(columnName + " (" + columnType + ")");
				}
				Collections.sort(attr);
				System.out.print("Attributes: ");
				for (int i = 0; i < attr.size(); i++) {
					System.out.print(attr.get(i));
					if (i != attr.size() - 1) {
						System.out.print(", ");
					}
				}
				System.out.println();
				// Pri Key
				ResultSet rs2 = dbmd.getPrimaryKeys(null, null, table);
				System.out.print("Primary Key: ");
				ArrayList<String> priKeys = new ArrayList<String>();
				while (rs2.next()) {
					String columnName = rs2.getString("COLUMN_NAME").toUpperCase();
					priKeys.add(columnName);
				}
				Collections.sort(priKeys);
				for (int i = 0; i < priKeys.size(); i++) {
					System.out.print(priKeys.get(i));
					if (i != priKeys.size() - 1) {
						System.out.print(", ");
					}
				}
				System.out.println();
			}
			System.out.println();
			System.out.println("### Joinable Pairs of Tables (based on Foreign Keys)");
			ArrayList<String> joinables = new ArrayList<String>();

			for (String table : tables) {
				DatabaseMetaData dbmd = connection.getMetaData();
				ResultSet rs3 = dbmd.getImportedKeys(null, null, table);
				while (rs3.next()) {
					String pTabName = rs3.getString("PKTABLE_NAME").toUpperCase();
					String pColName = rs3.getString("PKCOLUMN_NAME").toUpperCase();
					String fTabName = rs3.getString("FKTABLE_NAME").toUpperCase();
					String fColName = rs3.getString("FKCOLUMN_NAME").toUpperCase();
					joinables.add(pTabName + " can be joined " + fTabName + " on attributes " + pColName
							+ " and " + fColName);
				}
			}

			joinables.sort(null);
			for (String att : joinables) {
				System.out.println(att);
			}

			rs.close();
		} catch (SQLException e) {
			// System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

	}

	public static void main(String[] argv) {
		executeMetadata(argv[0]);
	}
}
