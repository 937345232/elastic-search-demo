package com.chenqin.elasticsearch.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class EsConfigUtils {
	private static String ipaddress;
	private static String port;
	private static String indexname;
	private static String type;
	
	/**
	 * ipaddress = 172.16.89.55
port = "9300"
indexname = "posts"
type = "doc"
	 */
	static {
		Properties prop = new Properties();
		try {
			InputStream in = EsConfigUtils.class.getClassLoader().getResourceAsStream("elasticseachsetting.properties");
			InputStreamReader reader = new InputStreamReader(in, "UTF-8");
			prop.load(reader);
			
			ipaddress = prop.getProperty("ipaddress").trim();
			port = prop.getProperty("port").trim();
			indexname =  prop.getProperty("indexname").trim();
			type = prop.getProperty("type").trim();
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	
	public static String getIpAddress(){
		return ipaddress;
	}
	
	public static String getPort(){
		return port;
	}
	
    public static String getIndexName(){
    	return indexname;
    }
    public static String getType(){
    	return type;
    }
	public static void main(String[] args) {
		System.out.println(EsConfigUtils.getIpAddress()+"--"+EsConfigUtils.getPort()+"---"+EsConfigUtils.getIndexName()+"--"+EsConfigUtils.getType());
	}


}
