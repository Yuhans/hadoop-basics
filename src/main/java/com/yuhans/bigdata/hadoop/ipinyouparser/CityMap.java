package com.yuhans.bigdata.hadoop.ipinyouparser;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class which processes file from Distributed Cache to create HashMap from it.
 * It contains public method to get name of a city by its id.
 *
 * @author Artem_Iushin <yushin.tema@gmail.com>
 */
public class CityMap {

    private Map<Long, String> cityIdToCityName = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(CityMap.class);

    public CityMap(FileSystem fileSystem, Path path) {
        createMap(fileSystem, path);
    }

    private void createMap(FileSystem fileSystem, Path path) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)))) {
            while (reader.ready()) {
                String[] cityIdToName = reader.readLine().split("\\s+");
                cityIdToCityName.put(Long.parseLong(cityIdToName[0]), cityIdToName[1]);
            }
        } catch (IOException e) {
            LOGGER.error("Can't get file by giving path");
        } catch (NumberFormatException e) {
            LOGGER.error("Error parsing Id to Integer");
        }
    }

    public String getCityNameByCityId(long cityId) {
        return cityIdToCityName.getOrDefault(cityId, String.format("No such cityId = {%d}", cityId));
    }
}
