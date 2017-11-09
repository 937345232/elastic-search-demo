package com.chenqin.elasticsearch.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContentUtils {
    public static boolean checkContentEn(String content){
    	if (content == null) {
			return false;
		}
    	Pattern p = Pattern.compile("^[A-Za-z]+$");
    	Matcher matcher = p.matcher(content.trim());
    	return matcher.matches();
    }
    
    public static List<String> splitContent(String content){
		ArrayList<String> result = new ArrayList<String>();
		if (content == null) {
			return result;
		}
		ArrayList<String> nameList = new ArrayList<String>();
		String reg = "[^\u4e00-\u9fa5]";
		content = content.trim().replaceAll(reg, "");
		System.out.println("------"+content);
		for (int i = 0 ; i < content.length() ; i++ ) {
			int j = i+1;
			nameList.add(content.substring(i,j));
		}
		for (int i = 0; i < content.length(); i++) {
			result.add(nameList.get(i));
			int j = i+1;
			if (j == content.length()) {
				break;
			}
		}
		return result;
	}

}
