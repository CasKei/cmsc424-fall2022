import java.sql.*;
import java.util.HashSet;
import java.util.ArrayList;

public class NearestNeighbor 
{
	static double jaccard(HashSet<String> s1, HashSet<String> s2) {
		int total_size = s1.size() + s2.size();
		int i_size = 0;
		for(String s: s1) {
			if (s2.contains(s))
				i_size++;
		}
		return ((double) i_size)/(total_size - i_size);
	}
	public static void executeNearestNeighbor() {
		/************* 
		 * Add your code to add a new column to the users table (set to null by default), calculate the nearest neighbor for each node (within first 5000), and write it back into the database for those users..
		 ************/
		Connection connection = null;
		try {
				connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/stackexchange","root", "root");
		} catch (SQLException e) {
				System.out.println("Connection Failed! Check output console");
				e.printStackTrace();
				return;
		}

		if (connection != null) {
				System.out.println("You made it, take control your database now!");
		} else {
				System.out.println("Failed to make connection!");
				return;
		}

		Statement add_col_nn = null;
		String add_col_nn_str = "alter table users add column nearest_neighbor integer;";

        return;
	}

	public static void main(String[] argv) {
		executeNearestNeighbor();
	}
}
