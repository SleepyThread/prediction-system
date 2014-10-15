package com.sleepythread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;

public class EventGeneratorMapper extends Mapper<Object, Text, NullWritable, Text> {

    public static final String HIVE_COLUMN_SEPARATOR = "\u0001";

    @Override
    public void map(Object key, Text record, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        DateTime endDate = DateTime.parse(configuration.get("endDate"),dateTimeFormatter);

        String[] split = record.toString().split(HIVE_COLUMN_SEPARATOR);

        Double id = Double.parseDouble(split[0]);
        Double interval = Double.parseDouble(split[1]);
        Double rate = Double.parseDouble(split[2]);
        Double current_usage = Double.parseDouble(split[3]);
        DateTime capture_date = DateTime.parse(split[4],dateTimeFormatter);

        Double nextServicingKm = Math.ceil(current_usage / interval)*rate;
        Double noOfDaysToReachNextServingKm = (nextServicingKm - current_usage)/rate;
        DateTime nextServicingDate = capture_date.plusDays(noOfDaysToReachNextServingKm.intValue());

        Double daysToCompleteIntervalUsingRate = (interval/rate);


        while (nextServicingDate.isBefore(endDate)){
            Text text = new Text(id+HIVE_COLUMN_SEPARATOR+nextServicingKm+HIVE_COLUMN_SEPARATOR+nextServicingKm);
            context.write(NullWritable.get(), text);

            nextServicingKm = nextServicingKm + interval;
            nextServicingDate = nextServicingDate.plusDays(daysToCompleteIntervalUsingRate.intValue());
        }
    }

}