package com.founder.orgcube.web.controller;



import com.founder.orgcube.utils.redis.RedisClient;
import net.sf.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;


/**
 * Created by wls on 2016/1/12.
 */

@Controller
@RequestMapping("/stats/*")
public class BehaviorController {
	@RequestMapping(value = "/showuser")
	public String userBehaviors(@RequestParam(required=true) String key){
		int dbIndex = 2;
		key = key+"*";
		JSONObject jsonObject = new JSONObject();
		Map<String,String> behaviorMap = RedisClient.getBehaviorToRedisByKey(dbIndex, key);
		jsonObject.putAll(behaviorMap);
		return jsonObject.toString();

	}
}
