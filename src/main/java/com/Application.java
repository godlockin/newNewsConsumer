package com;

import com.common.constants.BusinessConstants;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

	private static final String ACTIVE_KEY = "ACTIVE_KEY";

	public static void main(String[] args) {
		System.setProperty(ACTIVE_KEY,
				StringUtils.isBlank(System.getProperty(ACTIVE_KEY)) ?
						BusinessConstants.TasksConfig.CORE_JOBS_KEY :
						System.getProperty(ACTIVE_KEY));

		SpringApplication.run(Application.class, args);
	}
}
