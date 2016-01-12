/*
package com.founder.orgcube.web.servlet;



import com.founder.orgcube.web.redis.RedisClient;
import net.sf.json.JSONObject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;


*/
/**
 * Created by wls on 2016-01-11-17:22:18
 *//*

public class BehaviorServlet extends HttpServlet{
	@Override
	public void doGet(HttpServletRequest request,HttpServletResponse response)throws ServletException,IOException{
		//int dbIndex = 2;
		//String key = "schoolId:140004990classId:userId:3FC1F854-501D-CF46-82F2-B2D97E266883*";
		//Map<String,String> behaviorLists = RedisClient.getBehaviorToRedisByKey(dbIndex, key);
		String action = request.getParameter("action");

		*/
/*if("getHebaviorList".equals(action)) {
			getHebaviorList(request, response);
		}*//*


	}

	@Override
	public void doPost(HttpServletRequest request,HttpServletResponse response) throws ServletException,IOException{

		this.doGet(request,response);

	}

	*/
/*private JSONObject getHebaviorList(HttpServletRequest request,HttpServletResponse response)throws ServletException,IOException{
		response.setContentType("text/html");
		response.setCharacterEncoding("utf-8");
		int dbIndex = 2;
		String key = request.getParameter("key");
		key = key+"*";
		JSONObject jsonObject = new JSONObject();
		Map<String,String> behaviorMap = RedisClient.getBehaviorToRedisByKey(dbIndex, key);
		jsonObject.putAll(behaviorMap);
		return jsonObject;
	}*//*


}
*/
