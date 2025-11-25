package ru.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class MapTest {
    @Test
    void test(){
        String value = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";
        Map<String,Integer> map = new HashMap<>();
        String[] words = value.replaceAll("[^a-zA-Z ]", "")
                .toLowerCase()
                .split("\\s+");
        for(String foundWord: words){
            map.put(foundWord,Integer.valueOf(1));
        }
        Assertions.assertTrue(map.containsKey("amet"));
    }
}
