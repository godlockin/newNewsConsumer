package com;

import com.common.constants.BusinessConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class Application {
	private static final String ACTIVE_KEY = "ACTIVE_KEY";

	public static void main(String[] args) {

        System.setProperty(ACTIVE_KEY, (null == args || 0 >= args.length) ?
                BusinessConstants.TasksConfig.CORE_JOBS_KEY : args[0]);

		SpringApplication.run(Application.class, args);
	}
}
