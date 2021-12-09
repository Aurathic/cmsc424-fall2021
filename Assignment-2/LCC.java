import java.sql.*;
import java.util.*;

public class LCC 
{
    /**
     Take a string in format `userXXX`, and get the integer associated
     (in this case, )
     */
    /*
    public static int idNumberFromString(String userid) {
        return (int) Integer.parseInt(userid.substring);
    }
    */

	public static void executeLCC() {
		/************* 
		 * Add you code to compute LCC for each node, and write it back into the database.
		 ************/
        Connection connection = null;
        try {
            //connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/socialnetwork","postgres","postgres");
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/socialnetwork","vagrant","vagrant");
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

        Statement stmt = null;
        // Store friends in an adjacency list format
        LinkedHashMap<String, List<String>> map = new LinkedHashMap<>();
        // String which contain comma-separated values for all LCCs in order of users in 'user' relation
        HashMap<String, Double> lccs = new HashMap<>();

        try {
            // Add new column
            // (Commented out when testing, because SQL complains when inserting columns that already exist)
            stmt = connection.createStatement();
            stmt.execute("alter table users add lcc real;");

            // Add all of the users to the adjacency list
            stmt = connection.createStatement();
            ResultSet rs1 = stmt.executeQuery("select userid from users;");
            while (rs1.next()) {
                String userid = rs1.getString("userid");
                map.putIfAbsent(userid, new LinkedList<String>());
            }

            // Store each connection in friends in adjacency list
            stmt = connection.createStatement();
            ResultSet rs2 = stmt.executeQuery("select * from friends;");
            while (rs2.next()) {
                String userid1 = rs2.getString("userid1");
                String userid2 = rs2.getString("userid2");
                map.get(userid1).add(userid2);
            }

            stmt.close();

            // For every user's friends, find which of them are friends, incrementing a stored value
            for (Map.Entry<String, List<String>> userFriends : map.entrySet()) {
                // key is userFriends.getKey(), values are userFriends.getValue();
                List<String> friends = userFriends.getValue();
                double lcc;
                int k = friends.size();
                if (k > 0) {
                    int N = 0; // number of friends which are also friends
                    for (String friend1 : friends) {
                        List<String> friend1List = map.get(friend1);
                        for (String friend2 : friends) {
                            // Is there an edge between friend1 and friend2?
                            if (!friend1.equals(friend2) && friend1List.contains(friend2)) {
                                N++;
                            }
                        }
                    }
                    // Since both directions of edge are included in the data set,
                    // I divide the formula here by two (removing the 2 coefficient)
                    lcc = (float) N / (k * (k-1));
                } else {
                    lcc = 0;
                }
                // Add LCC to HashMap
                lccs.put(userFriends.getKey(), lcc);
            }
            
            // Add data into column 
            PreparedStatement pstmt = connection.prepareStatement("update users set lcc = ? where userid = ?");
            // Add each user to update to batch prepared statement
            for (Map.Entry<String, Double> userLCC : lccs.entrySet()) {
                pstmt.setDouble(1, userLCC.getValue());
                pstmt.setString(2, userLCC.getKey());
                pstmt.addBatch();
            }

            pstmt.executeBatch();

            pstmt.close();
        } catch (SQLException e ) {
            System.out.println(e);
        }
	}

	public static void main(String[] argv) {
        executeLCC();
	}
}
