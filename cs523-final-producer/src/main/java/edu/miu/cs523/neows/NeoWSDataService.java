package edu.miu.cs523.neows;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Service
public class NeoWSDataService {

    @Autowired
    private RestTemplate restTemplate;

    @Value(value = "${nasa.api_key}")
    private String apiKey;

    public NeoWSData fetchData(LocalDate dateToFetch) {
        String endpoint = "https://api.nasa.gov/neo/rest/v1/feed";
        StringBuilder urlBuilder = new StringBuilder(endpoint);
        String dateStr = dateToFetch.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        urlBuilder.append("?").append("start_date=").append(dateStr).append("&end_date=").append(dateStr).append("&api_key=").append(apiKey);
        return restTemplate.getForObject(urlBuilder.toString(), NeoWSData.class);
    }
}
