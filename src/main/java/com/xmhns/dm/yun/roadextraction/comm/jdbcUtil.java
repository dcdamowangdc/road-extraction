package com.xmhns.dm.yun.roadextraction.comm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class jdbcUtil {
    public static void DeleteRow(String url,String username,String password,String delete_sql) {
        Connection connection = null;
        PreparedStatement st = null;
//        String url = "jdbc:oracle:thin:@192.168.50.26:1521/hnscan";
//        String username = "can_analysis";
//        String password = "can_analysis";
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(url, username, password);
            st = connection.prepareStatement(delete_sql);
            st.executeUpdate();
        } catch(Exception e) {
            System.out.println(e);
        }finally {
            try {
                st.close();
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}