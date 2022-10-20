import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

public class NearestNeighbor {
	static double jaccard(HashSet<String> s1, HashSet<String> s2) {
		int total_size = s1.size() + s2.size();
		int i_size = 0;
		for (String s : s1) {
			if (s2.contains(s))
				i_size++;
		}
		return ((double) i_size) / (total_size - i_size);
	}

	public static void executeNearestNeighbor() {
		/*************
		 * Add your code to add a new column to the users table (set to null by
		 * default), calculate the nearest neighbor for each node (within first 5000),
		 * and write it back into the database for those users..
		 ************/
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/stackexchange", "root", "root");
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

		// 1. add col to users: nn(int)
		Statement add_col_nn = null;
		String add_col_nn_str = "alter table users add column nearest_neighbor integer default null;";
		try {
			add_col_nn = connection.createStatement();
			add_col_nn.executeUpdate(add_col_nn_str);
			System.out.println("Added column nearest_neighbours to users");
			connection.commit();
			add_col_nn.close();
		} catch (SQLException e) {
			System.out.println(e);
		}

		// 2. get data. for each user, an array of tags strings. users with no posts
		// with tags will be omitted
		Statement stmt2 = null;
		String query2 = "SELECT users.id, array_remove(array_agg(posts.tags), null) AS arr FROM users, posts WHERE users.id = posts.OwnerUserId AND users.id < 5000 GROUP BY users.id HAVING count(posts.tags) > 0;";
		Map<Integer, HashSet<String>> stash = new HashMap<>();

		try {
			stmt2 = connection.createStatement();
			ResultSet rs = stmt2.executeQuery(query2);

			while (rs.next()) {
				// parse and separate tags for each user
				String str = rs.getString("arr");
				Integer id = rs.getInt("id");
				HashSet<String> h = new HashSet<String>();

				str = str.replaceAll("\\{", "");
				str = str.replaceAll("\\}", "");
				str = str.replaceAll(",", "");
				str = str.replaceAll(">", "");

				String[] tag = str.split("<");
				for (String t : tag) {
					System.out.println(t);
					h.add(t);
				}

				stash.put(id, h);
			}
			connection.commit();
			stmt2.close();

		} catch (SQLException e) {
			System.out.println(e);
		}

		// stash <id, hashset{tags}>

		// for each user, go through the rest of the users and find
		// user with highest Jacc based on the tag sets of the 2 users

		for (int id : stash.keySet()) {
			double max_jaccard = 0;
			int nearest_neighbor = -1;
			for (int id2 : stash.keySet()) {
				if (id == id2)
					continue;
				double jaccard = jaccard(stash.get(id), stash.get(id2));
				if (jaccard > max_jaccard) {
					max_jaccard = jaccard;
					nearest_neighbor = id2;
				} else if (jaccard == max_jaccard) {
					if (id2 < nearest_neighbor) {
						nearest_neighbor = id2;
					}
				}
			}
			// update the nearest neighbor in the database
			String update_query = "update users set nearest_neighbor = " + nearest_neighbor + " where id = " + id
					+ ";";
			System.out.println(update_query);
			Statement stmt = null;
			try {
				stmt = connection.createStatement();
				stmt.executeUpdate(update_query);
				connection.commit();
				stmt.close();
			} catch (SQLException e) {
				System.out.println(e);
			}

		}
		return;
	}

	public static void main(String[] argv) {
		executeNearestNeighbor();
	}
}
