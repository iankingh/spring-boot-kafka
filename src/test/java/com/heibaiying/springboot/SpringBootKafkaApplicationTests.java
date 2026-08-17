package com.heibaiying.springboot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.heibaiying.springboot.bean.Programmer;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class SpringBootKafkaApplicationTests {

    @Test
    public void programmerJsonRoundTrip() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Programmer expected = new Programmer("xiaoming", 12, 21212.33f, new Date(1560935191543L));

        String json = objectMapper.writeValueAsString(expected);
        Programmer actual = objectMapper.readValue(json, Programmer.class);

        assertEquals(expected, actual);
    }
}
