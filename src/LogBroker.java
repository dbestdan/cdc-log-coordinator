import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LogBroker implements Config {
	
	private Map<Timestamp, HashSet<Long>> map = null;
	private ServerSocket logSocketServer1;
	private Socket logSocketClient1;
	private DataInputStream input1;
	private PrintStream output1;
	private ServerSocket logSocketServer2;
	private Socket logSocketClient2;
	private DataInputStream input2;
	private PrintStream output2;
	private long sessionStartTime = 0L;
	private long sessionEndTime = 0L;
	private Connection conn = null;
	private PreparedStatement prep = null;
	private int changeCount = 0;
	private long sleepDuration = 0L;
	private long transactionId = 0L;
	private Writer stalenessOutPut;
	public static Timestamp uptodate = null;
	private long cummulativeStaleness = 0L;
	private long averageStaleness = 0L;
	private int count =0;


	public LogBroker(long sessionStartTime, long sessionEndTime) {
		this.sessionStartTime = sessionStartTime;
		this.sessionEndTime = sessionEndTime;
		this.conn = Server.getConnection();
		this.map = new HashMap<Timestamp, HashSet<Long>>();
		this.changeCount = Integer.parseInt(System.getProperty("changeCount"));
		this.sleepDuration = Long.parseLong(System.getProperty("sleepDuration"));
		String query = "SELECT xid, data FROM pg_logical_slot_get_changes('replication_slot', NULL, " + changeCount
				+ ")";

		try {
			prep = conn.prepareStatement(query);

			// initialization for two socket binding
			logSocketServer1 = new ServerSocket(9999);
			logSocketServer2 = new ServerSocket(9998);
			logSocketClient1 = logSocketServer1.accept();
			logSocketClient2 = logSocketServer2.accept();
			input1 = new DataInputStream(logSocketClient1.getInputStream());
			input2 = new DataInputStream(logSocketClient2.getInputStream());
			output1 = new PrintStream(logSocketClient1.getOutputStream());
			output2 = new PrintStream(logSocketClient2.getOutputStream());

			// initializing writer for staleness
			String stalenessFileName = "staleness_log_based_sleep_duration_" + sleepDuration +"_change_count_" + changeCount
					+ "_" + dateFormat.format(new Date());

			stalenessOutPut = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(stalenessFileName, true), "UTF-8"));
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void readLog() {
		Set<Long> ids = new HashSet<Long>();
		while (System.currentTimeMillis() < sessionEndTime) {
			ids.clear();
			

			//sleep for specified time
			try {
				Thread.sleep(sleepDuration);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
			//write the data to the socket
			try {
				
				boolean isTableFound = false;
				ResultSet rs = prep.executeQuery();
				while (rs.next()) {
					ids.add(rs.getLong(1));
					String data = rs.getString(2);
					isTableFound = false;
					for (String tableName : table_group_1) {
						if (data.contains(tableName)) {
							isTableFound = true;
							output1.append(data + "\n");
							break;
						}
					}
					// check if table is already found in first group of tables
					if (!isTableFound) {
						for (String tableName : table_group_2) {
							if (data.contains(tableName)) {
								output2.append(data + "\n");
								break;
							}
						}
					}
				}
				output1.flush();
				output2.flush();
				
				//record the staleness based on latest transaction id
				
				for (Long id : ids) {
					rs = conn.createStatement().executeQuery("select pg_xact_commit_timestamp('" + id + "'::xid)");
					rs.next();
					Timestamp t = rs.getTimestamp(1);
					if (map.keySet().contains(t)) {
						map.get(t).add(id);
					} else {
						HashSet<Long> set = new HashSet<Long>();
						set.add(id);
						map.put(t, set);
					}
				}
				uptodate = Collections.max(map.keySet());
				
				
//				rs = conn.createStatement()
//						.executeQuery("select pg_xact_commit_timestamp('" + transactionId + "'::xid)");
//				rs.next();
//				Timestamp t = rs.getTimestamp(1);

				long staleness = System.currentTimeMillis() - uptodate.getTime();
				count++;
				cummulativeStaleness += staleness;
				averageStaleness = cummulativeStaleness/count;
				
				stalenessOutPut.append((System.currentTimeMillis() - sessionStartTime) + "," + averageStaleness + "\n");
				stalenessOutPut.flush();
			

			} catch (SQLException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

}
