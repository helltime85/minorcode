import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.sqlite.SQLiteConfig;

import obj.ColumnInfo;

public class DBUtil {
	private static final Logger logger = Logger.getLogger(DBUtil.class);	
	
	@SuppressWarnings({ "static-access", "unchecked" })
	public static JSONArray DBToJSON(ResultSet rs) {
		JSONArray jsonArray = null;
		JSONObject jsonObject = null;
		try {
			if(rs == null) {
				throw new Exception("ResultSet is null");
			}
			
			List<ColumnInfo> columnInfoList = getColumnInfoList(rs);
			
			jsonArray = new JSONArray();
			
			while(rs.next()) {
				if(columnInfoList.size() > 0) {
					jsonObject = new JSONObject();
					
					for(int i=0;i<columnInfoList.size();i++) {
						int columnType = columnInfoList.get(i).getColumnType();
						
						switch(columnType) {				
							case Types.NUMERIC :											
							case Types.INTEGER :
								jsonObject.put(columnInfoList.get(i).getColumnName(), rs.getInt(columnInfoList.get(i).getColumnName()));
							case Types.FLOAT :
								jsonObject.put(columnInfoList.get(i).getColumnName(), rs.getFloat(columnInfoList.get(i).getColumnName()));
							case Types.DOUBLE :
								jsonObject.put(columnInfoList.get(i).getColumnName(), rs.getDouble(columnInfoList.get(i).getColumnName()));
							case Types.DATE :
								jsonObject.put(columnInfoList.get(i).getColumnName(), rs.getDate(columnInfoList.get(i).getColumnName()));
							case Types.CHAR :
							default :
								jsonObject.put(columnInfoList.get(i).getColumnName(), rs.getString(columnInfoList.get(i).getColumnName()));
						}  //switch end
					}
					
					jsonArray.add(jsonObject);
				} else {
					throw new Exception("columnInfo not found");
				}	
			}
			

		} catch(Exception e) {
			logger.info("Exception : " + e.toString());
		}
		
		return jsonArray;
	}
 	
	public static ResultSet selectList(String SQLString, List<ColumnData> param) {
		ResultSet rs = null;
		Boolean isEmpty = SQLString.trim().isEmpty();
		
		try {			
			if(!isEmpty) {
				Connection conn = getConnection();
				PreparedStatement pstm = conn.prepareStatement(SQLString);
				ColumnData cd = null;
				
				for(int i=0;i<param.size();i++) {
					cd = param.get(i);

					switch(ColumnInfo.getColumnType()) {				
					case Types.NUMERIC :											
					case Types.INTEGER :
						pstm.setInt(i, (int)cd.getColumnData()); 
					case Types.FLOAT :
						pstm.setFloat(i, (Float)cd.getColumnData());
					case Types.DOUBLE :
						pstm.setDouble(i, (Double)cd.getColumnData());
					case Types.DATE :
						pstm.setDate(i, (Date)cd.getColumnData());
					case Types.CHAR :
					default :
						pstm.setString(i, (String)cd.getColumnData());
					} 
					
				}
	 
				rs = pstm.executeQuery();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return rs;
	}
	
	public static ColumnInfo getColumnInfo(ResultSetMetaData rsmd, int idx) {
		ColumnInfo columnInfo = null;
		
		try {			
			ColumnInfo.setTableName(rsmd.getTableName(idx));
			ColumnInfo.setColumnName(rsmd.getColumnName(idx));
			ColumnInfo.setColumnType(rsmd.getColumnType(idx));
			ColumnInfo.setColumnDisplaySize(rsmd.getColumnDisplaySize(idx));
			
			columnInfo = new ColumnInfo();
		} catch(SQLException se) {
			logger.info("SQLException : " + se.toString());
		} catch(Exception e) {
			logger.info("Exception : " + e.toString());
		}
		
		return columnInfo;
	}
	
	public static List<ColumnInfo> getColumnInfoList(ResultSet rs) {
		List<ColumnInfo> columnInfoList = null;
		ColumnInfo columnInfo = null;
		
		try {
			columnInfoList = new ArrayList<ColumnInfo>();
			ResultSetMetaData rsmd = rs.getMetaData();
			
			int colCnt = rsmd.getColumnCount();
			
			for(int i=0;i<colCnt;i++) {
				columnInfo = new ColumnInfo();
				columnInfo = getColumnInfo(rsmd, i);
				
				columnInfoList.add(columnInfo);
			}

		} catch(SQLException se) {
			logger.info("SQLException : " + se.toString());
		} catch(Exception e) {
			logger.info("Exception : " + e.toString());
		}
		
		return columnInfoList;
	} 
	
	public static Connection getConnection() {
		SQLiteConfig config = new SQLiteConfig();
		Connection conn = null;
		int retryCnt = 0;
		int maxCnt = 10;
		
		while(true) {
			try {
				if(retryCnt >= maxCnt) {
					logger.info("DB Conneciton Failed");
					throw new SQLException("DB Conneciotn Failed");
				}
				
				logger.info("DB Connection try(" +( retryCnt+1) + "/" + maxCnt + ")");
				conn = DriverManager.getConnection("jdbc:sqlite:SQLDB.db", config.toProperties());
				
				if(retryCnt == 0) {
					break;
				} else {
					Thread.sleep(3000);					
				}
			} catch(SQLException se) {
				retryCnt++;
			} catch(Exception e) {
				retryCnt++;
			}
		} 
		
		logger.info("Connection to SQLite has been established");
		return conn;	
	}
	
	public static void closeConnection(Connection conn, PreparedStatement pstm, ResultSet rs) throws SQLException {
		try {		
			if(rs != null) {
				try { 
					rs.close();   
				} catch(SQLException se) { 
					throw new SQLException(ignore.toString()); 
				}
			}
			
		    if(pstm != null) {
				try { 
					pstm.close(); 
				} catch(SQLException se) { 
					throw new SQLException(ignore.toString()); 
				}
			}
			
		    if(conn != null) {
				try { 
					conn.close(); 
				} catch(SQLException se) { 
					throw new SQLException(ignore.toString()); 
				}
			}
			
		    logger.info("DB close finishied");
		} catch(SQLException se) {
			logger.info("DB close faiied[" + se.toString() + "]");
			throw new SQLException(se.toString());
		}
	}
}
