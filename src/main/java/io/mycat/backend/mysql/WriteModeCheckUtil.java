package io.mycat.backend.mysql;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.net.mysql.ErrorPacket;

public class WriteModeCheckUtil {
	private WriteModeCheckUtil(){}
	
	public static void check(ErrorPacket err, String errMsg, BackendConnection conn){
		if(conn instanceof MySQLConnection){
			MySQLConnection mysqlConn = (MySQLConnection) conn;
			
			if(!conn.isFromSlaveDB() && mysqlConn.getPool().getConfig().isCheckWriteMode()){
				if(isReadOnlyError(err, errMsg)){
					conn.close("the connection was unavailable for writing and it will be closed due to checkWriteMode was true and being created from write host");
				}
			}
		}
	}
	
	private static boolean isReadOnlyError(ErrorPacket err, String errMsg){
		//the read-only exception we found from AWS RDS was '1290', '1209' could be the original error set by mysql's 'read-only' parameter (not yet tested)
		if(err.errno == 1290 || err.errno == 1209){
			if(errMsg.toLowerCase().indexOf("read-only") >= 0){
				return true;
			}
		}
		
		return false;
	}
}
