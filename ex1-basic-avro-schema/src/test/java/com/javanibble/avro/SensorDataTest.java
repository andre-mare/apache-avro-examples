package com.javanibble.avro;


import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;


class SensorDataTest {


    @Test
    void sensorDataInstantiationTest() {
        SensorData sensorData = new SensorData();
        long timestamp = System.currentTimeMillis();
        ByteBuffer firmware = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});

        sensorData.setSensorId("sensor_1");
        sensorData.setTimestamp(timestamp);
        sensorData.setIsActive(true);
        sensorData.setTemperature(26.2f);
        sensorData.setHumidity(80.0);
        sensorData.setFirmwareVersion(firmware);
        sensorData.setStatusMessage("Operating normally");
        sensorData.setErrorCount(0);

        assertNotNull(sensorData, "The sensorData object should not be null.");

        assertEquals("sensor_1", sensorData.getSensorId(), "The sensorId does not match the expected value.");
        assertEquals(timestamp, sensorData.getTimestamp(), "The timestamp does not match the expected value.");
        assertEquals(true, sensorData.getIsActive(), "The isActive does not match the expected value.");
        assertEquals(26.2f, sensorData.getTemperature(), "The temperature does not match the expected value.");
        assertEquals(80.0, sensorData.getHumidity(), "The humidity does not match the expected value.");
        assertEquals(firmware, sensorData.getFirmwareVersion(), "The firmware version does not match the expected value.");
        assertEquals("Operating normally", sensorData.getStatusMessage(), "The status message does not match the expected value.");
        assertEquals(0, sensorData.getErrorCount(), "The error count does not match the expected value.");
    }
}