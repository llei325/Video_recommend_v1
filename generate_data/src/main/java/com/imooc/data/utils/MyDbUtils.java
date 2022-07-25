package com.imooc.data.utils;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class MyDbUtils {// 拒绝继承
	private static Logger logger = LoggerFactory.getLogger(MyDbUtils.class);
	private static String className = "";
	private static String url = "";
	private static String user = "";
	private static String password = "";

	static{
		try{
			Properties prop = new Properties();
			prop.load(MyDbUtils.class.getClassLoader().getResourceAsStream("db.properties"));
			className = prop.getProperty("className");
			url = prop.getProperty("url");
			user = prop.getProperty("user");
			password = prop.getProperty("password");
		}catch (Exception e){
			logger.error("db.properties文件读取异常..."+e.getMessage());
		}
	}

	private static QueryRunner queryRunner = new QueryRunner();

	// 拒绝new一个实例
	private MyDbUtils() {
	};

	static {// 调用该类时既注册驱动
		try {
			Class.forName(className);
		} catch (Exception e) {
			logger.error("DriverClass注册失败..."+e.getMessage());
			throw new RuntimeException();
		}
	}

	public static void main(String[] args) {
		List<Object[]> objects = executeQuerySql("select * from user");
		System.out.println(objects.size());
	}

	public static List<Object[]> executeQuerySql(String sql) {
		List<Object[]> result = new ArrayList<Object[]>();
		try {
			List<Object[]> requstList = queryRunner.query(getConnection(), sql,
					new ArrayListHandler(new BasicRowProcessor() {
						@Override
						public <Object> List<Object> toBeanList(ResultSet rs,
								Class<Object> type) throws SQLException {
							return super.toBeanList(rs, type);
						}
					}));
			for (Object[] objects : requstList) {
				result.add(objects);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("sql查询失败:"+sql+","+e.getMessage());
		}
		return result;
	}

	public static void update(String sql) {
		Connection connection =null;
		try {
			connection = getConnection();
			queryRunner.update(connection, sql);
		} catch (SQLException e) {
			logger.error("sql更新失败:"+sql+","+e.getMessage());
		}finally {
			if(connection!=null){
				try {
					connection.close();
				}catch (Exception e){
					logger.error("conn链接关闭失败"+e.getMessage());
				}
			}
		}
	}

	public static void batchUpdate(List<String> sqlList) {
		Connection connection =null;
		try {
			connection = getConnection();
			connection.setAutoCommit(false);
			Statement state = connection.createStatement();
			for (String sql: sqlList) {
				state.addBatch(sql);
			}
			state.executeBatch();
			connection.commit();
		} catch (SQLException e) {
			logger.error("sql批量执行失败,"+e.getMessage()+","+sqlList.get(0));
		}finally {
			if(connection!=null){
				try {
					connection.close();
				}catch (Exception e){
					logger.error("conn链接关闭失败"+e.getMessage());
				}
			}
		}
	}

	// 获取连接
	private static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, user, password);
	}



}