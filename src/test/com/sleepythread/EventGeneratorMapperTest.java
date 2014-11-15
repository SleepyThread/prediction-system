package com.sleepythread;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static com.sleepythread.EventGeneratorMapper.HIVE_COLUMN_SEPARATOR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.powermock.api.mockito.PowerMockito.when;

public class EventGeneratorMapperTest {


  @Test
  public void shouldGenerateEventsForGivenVehicle() throws IOException, InterruptedException {
    Text record = new Text("1234" + HIVE_COLUMN_SEPARATOR + "10000" + HIVE_COLUMN_SEPARATOR +
                           "100" + HIVE_COLUMN_SEPARATOR + "1000" + HIVE_COLUMN_SEPARATOR + "2014-01-01");

    EventGeneratorMapper eventGeneratorMapper = new EventGeneratorMapper();
    Mapper.Context context = mock(Mapper.Context.class);

    Configuration configuration = mock(Configuration.class);
    when(context.getConfiguration()).thenReturn(configuration);
    when(configuration.get("endDate")).thenReturn("2015-01-01");

    eventGeneratorMapper.map(new Text(), record, context);

    Text expectedEvent1 = new Text("1234"+HIVE_COLUMN_SEPARATOR+"100"+HIVE_COLUMN_SEPARATOR+"2013-12-23");
    Text expectedEvent2 = new Text("1234"+HIVE_COLUMN_SEPARATOR+"10100"+HIVE_COLUMN_SEPARATOR+"2014-04-02");
    Text expectedEvent3 = new Text("1234"+HIVE_COLUMN_SEPARATOR+"20100"+HIVE_COLUMN_SEPARATOR+"2014-07-11");
    Text expectedEvent4 = new Text("1234"+HIVE_COLUMN_SEPARATOR+"30100"+HIVE_COLUMN_SEPARATOR+"2014-10-19");

    verify(context).write(NullWritable.get(), expectedEvent1);
    verify(context).write(NullWritable.get(), expectedEvent2);
    verify(context).write(NullWritable.get(), expectedEvent3);
    verify(context).write(NullWritable.get(), expectedEvent4);
  }

  @Test
  public void shouldNotGenerateAnyEventsIfUtilizationIsNull() throws IOException, InterruptedException {
    Text record = new Text("1234" + HIVE_COLUMN_SEPARATOR + "10000" + HIVE_COLUMN_SEPARATOR +
      "0" + HIVE_COLUMN_SEPARATOR + "1000" + HIVE_COLUMN_SEPARATOR + "2014-01-01");

    EventGeneratorMapper eventGeneratorMapper = new EventGeneratorMapper();
    Mapper.Context context = mock(Mapper.Context.class);

    Configuration configuration = mock(Configuration.class);
    when(context.getConfiguration()).thenReturn(configuration);
    when(configuration.get("endDate")).thenReturn("2015-01-01");

    eventGeneratorMapper.map(new Text(), record, context);


    verify(context).getConfiguration();
    verify(context).getConfiguration();
    verifyNoMoreInteractions(context);

  }

}
