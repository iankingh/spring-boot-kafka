package com.heibaiying.springboot;

import com.heibaiying.springboot.bean.Programmer;
import org.junit.jupiter.api.Test;

import java.util.Date;
import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SpringBootKafkaApplicationTests {

    @Test
    public void programmerJsonRoundTrip() {
        ObjectMapper objectMapper = new ObjectMapper();
        Programmer expected = new Programmer("xiaoming", 12, 21212.33f, new Date(1560935191543L));

        String json = objectMapper.writeValueAsString(expected);
        Programmer actual = objectMapper.readValue(json, Programmer.class);

        assertEquals(expected, actual);
    }
}
