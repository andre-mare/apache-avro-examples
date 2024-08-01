package com.example.avro;

import com.example.avro.SensorData;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The following class makes use of the instructions found on the Apache Avro Documentation.
 * @See: <a href="https://avro.apache.org/docs/1.11.1/getting-started-java/">Apache Avro: Getting Started with Java</a>
 */
public class SpecificRecordBasicExamples {

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
        SensorData sensorData = new SensorData("sensor_2",
                System.currentTimeMillis(),
                true,
                23.6f,
                78.0,
                ByteBuffer.wrap(new byte[]{1, 2, 3, 4}),
                "Operating normally",
                0);

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



    public static void objectSerializing(SensorData data1, SensorData data2, SensorData data3) throws Exception {
        File avroFile = getTargetDirectoryPath().resolve("sensordata.avro").toFile();
        DatumWriter<SensorData> sensorDataDatumWriter = new SpecificDatumWriter<SensorData>(SensorData.class);
        DataFileWriter<SensorData> dataFileWriter = new DataFileWriter<SensorData>(sensorDataDatumWriter);

        dataFileWriter.create(data1.getSchema(), avroFile);
        dataFileWriter.append(data1);
        dataFileWriter.append(data2);
        dataFileWriter.append(data3);
        dataFileWriter.close();
    }


    public static void objectDeserializing() throws Exception {
        File avroFile = getTargetDirectoryPath().resolve("sensordata.avro").toFile();
        DatumReader<SensorData> sensorDataDatumReader = new SpecificDatumReader<SensorData>(SensorData.class);
        DataFileReader<SensorData> dataFileReader = new DataFileReader<SensorData>(avroFile, sensorDataDatumReader);
        SensorData sensorData = null;
        while (dataFileReader.hasNext()) {
            sensorData = dataFileReader.next(sensorData);
            System.out.println(sensorData);
        }
    }


    public static Path getTargetDirectoryPath() throws Exception{
        Path classFilePath = Paths.get(GenericRecordBasicExamples.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path moduleRootPath = classFilePath.getParent().getParent();

        return moduleRootPath.resolve("target/");
    }


    public static void main(String[] args) throws Exception {
        SensorData sensorData1 = SpecificRecordBasicExamples.objectCreationType1();
        SensorData sensorData2 = SpecificRecordBasicExamples.objectCreationType2();
        SensorData sensorData3 = SpecificRecordBasicExamples.objectCreationType3();

        SpecificRecordBasicExamples.objectSerializing(sensorData1, sensorData2, sensorData3);
        SpecificRecordBasicExamples.objectDeserializing();

    }
}
