package com;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		System.setProperty("ACTIVE_KEY",
				StringUtils.isBlank(System.getProperty("ACTIVE_KEY")) ?
						"monthly" : System.getProperty("ACTIVE_KEY"));

		SpringApplication.run(Application.class, args);
	}
}
