package com.example.nsproject.monitoring;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MetricsPushScheduler {

    @Autowired
    private PrometheusMeterRegistry prometheusMeterRegistry;
    @Value("${url.vm}")
    private String urlVM;

    @Scheduled(fixedRate = 30000)
    public void pushMetrics() {
        String metricsData = prometheusMeterRegistry.scrape();

//        log.info(metricsData);//Использовать для отладки
        try {
            java.net.URL url = new java.net.URL(urlVM);
            java.net.HttpURLConnection connection = (java.net.HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "text/plain");

            String auth = "insert:insert";
            String encoding = java.util.Base64.getEncoder().encodeToString(auth.getBytes());
            connection.setRequestProperty("Authorization", "Basic " + encoding);

            try (java.io.OutputStream os = connection.getOutputStream()) {
                os.write(metricsData.getBytes());
                os.flush();
            }

            int responseCode = connection.getResponseCode();
//            log.info("Push response: " + responseCode);//Ответ от сервера (204-Ок без response)

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
