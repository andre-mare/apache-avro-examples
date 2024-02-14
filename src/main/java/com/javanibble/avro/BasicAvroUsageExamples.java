package com.javanibble.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * The following class makes use of the instructions found on the Apache Avro Documentation.
 * @See: https://avro.apache.org/docs/1.10.2/gettingstartedjava.html
 */
public class BasicAvroUsageExamples {

    public static SensorData objectCreationType1() {
        SensorData sensorData = new SensorData();

        sensorData.setSensorId("sensor_1");
        sensorData.setTimestamp(System.currentTimeMillis());
        sensorData.setIsActive(true);
        sensorData.setTemperature(26.2f);
        sensorData.setHumidity(80.0);
        sensorData.setFirmwareVersion(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        sensorData.setStatusMessage("Operating normally");
        sensorData.setErrorCount(0);
        return sensorData;
    }


    public static SensorData objectCreationType2() {
        SensorData sensorData = new SensorData("sensor_2", System.currentTimeMillis(), true, 23.6f, 78.0, ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), "Operating normally", 0);

        return sensorData;
    }


    public static SensorData objectCreationType3() {
        SensorData sensorData = SensorData.newBuilder()
                .setSensorId("sensor_3")
                .setTimestamp(System.currentTimeMillis())
                .setIsActive(true)
                .setTemperature(22.2f)
                .setHumidity(65.0)
                .setFirmwareVersion(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))
                .setStatusMessage("Operating normally")
                .setErrorCount(0)
                .build();

        return sensorData;
    }


    public static GenericRecord objectCreationType4() throws Exception {
        InputStream inputStream = BasicAvroUsageExamples.class.getClassLoader().getResourceAsStream("avro/sensordata.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);

        GenericRecord sensorData = new GenericData.Record(schema);

        sensorData.put("sensorId", "sensor_4");
        sensorData.put("timestamp", System.currentTimeMillis());
        sensorData.put("isActive", true);
        sensorData.put("temperature", 21.3f);
        sensorData.put("humidity", 90.2);
        sensorData.put("firmwareVersion", ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        sensorData.put("statusMessage", "Operating normally");
        sensorData.put("errorCount", 0);
        return sensorData;
    }

    public static void objectSerializing(SensorData data1, SensorData data2, SensorData data3) throws Exception {
        DatumWriter<SensorData> sensorDataDatumWriter = new SpecificDatumWriter<SensorData>(SensorData.class);
        DataFileWriter<SensorData> dataFileWriter = new DataFileWriter<SensorData>(sensorDataDatumWriter);

        dataFileWriter.create(data1.getSchema(), new File("sensordata.avro"));
        dataFileWriter.append(data1);
        dataFileWriter.append(data2);
        dataFileWriter.append(data3);
        dataFileWriter.close();
    }


    public static void objectDeserializing() throws Exception {
        DatumReader<SensorData> sensorDataDatumReader = new SpecificDatumReader<SensorData>(SensorData.class);
        DataFileReader<SensorData> dataFileReader = new DataFileReader<SensorData>(new File("sensordata.avro"), sensorDataDatumReader);
        SensorData sensorData = null;
        while (dataFileReader.hasNext()) {
            sensorData = dataFileReader.next(sensorData);
            System.out.println(sensorData);
        }
    }

    public static void genericRecordSerializing(GenericRecord sensorData) throws Exception {
        InputStream inputStream = BasicAvroUsageExamples.class.getClassLoader().getResourceAsStream("avro/sensordata.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, new File("generic-sensordata.avro"));
        dataFileWriter.append(sensorData);
        dataFileWriter.close();
    }


    public static void genericRecordDeserializing() throws Exception {
        InputStream inputStream = BasicAvroUsageExamples.class.getClassLoader().getResourceAsStream("avro/sensordata.avsc");
        Schema schema = new Schema.Parser().parse(inputStream);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("generic-sensordata.avro"), datumReader);
        GenericRecord sensorData = null;
        while (dataFileReader.hasNext()) {
            sensorData = dataFileReader.next(sensorData);
            System.out.println(sensorData);
        }
    }



    public static void main(String[] args) throws Exception {
        SensorData sensorData1 = BasicAvroUsageExamples.objectCreationType1();
        SensorData sensorData2 = BasicAvroUsageExamples.objectCreationType2();
        SensorData sensorData3 = BasicAvroUsageExamples.objectCreationType3();

        BasicAvroUsageExamples.objectSerializing(sensorData1, sensorData2, sensorData3);
        BasicAvroUsageExamples.objectDeserializing();

        GenericRecord genericRecord = BasicAvroUsageExamples.objectCreationType4();
        BasicAvroUsageExamples.genericRecordSerializing(genericRecord);
        BasicAvroUsageExamples.genericRecordDeserializing();

    }
}
