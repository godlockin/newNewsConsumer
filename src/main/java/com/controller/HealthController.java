package com.controller;

import com.service.MonitorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HealthController {

    @Autowired
    private MonitorService monitorService;

    @RequestMapping(value = "/_health")
    public int healthCheck() {
        return 200;
    }

    @RequestMapping(value = "/_statement", method = RequestMethod.GET)
    public Map monitor() {
        return monitorService.statement();
    }
}
