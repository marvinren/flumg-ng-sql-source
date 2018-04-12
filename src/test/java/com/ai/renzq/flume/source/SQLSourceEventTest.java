package com.ai.renzq.flume.source;

import com.ai.renzq.flume.eventsource.SQLSourceEvent;
import com.google.common.collect.ImmutableMap;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @ProjectName: flumengsqlsource
 * @Package: com.ai.renzq.flume.source
 * @ClassName: ${TYPE_NAME}
 * @Description:
 * @Author: Renzq
 * @CreateDate: 2018/4/11 18:55
 * @UpdateUser: Renzq
 * @UpdateDate: 2018/4/11 18:55
 * @UpdateRemark:
 * @Version: 1.0
 * Copyright: Copyright (c) 2018
 **/
public class SQLSourceEventTest {

    private static SQLSourceEvent fixture;
    private static Context context = mock(Context.class);

    @SuppressWarnings("rawtypes")
    private static ChannelProcessor mockChannelProcessor;

    @BeforeClass
    public static void setUp() throws NoSuchFieldException, IllegalAccessException {
        when(context.getString("status.file.name")).thenReturn("statusFileName.txt");
        when(context.getString("hibernate.connection.url")).thenReturn("jdbc:oracle:thin:@10.1.195.102:1521/aidb");
        when(context.getString("table")).thenReturn("table");
        when(context.getString("incremental.column.name")).thenReturn("incrementalColumName");
        when(context.getString("status.file.path", "/var/lib/flume")).thenReturn("/tmp/flume");
        when(context.getString("columns.to.select", "*")).thenReturn("*");
        when(context.getInteger("run.query.delay", 10000)).thenReturn(10000);
        when(context.getInteger("batch.size", 100)).thenReturn(100);
        when(context.getInteger("max.rows", 10000)).thenReturn(10000);
        when(context.getString("incremental.value", "0")).thenReturn("0");
        when(context.getString("start.from", "0")).thenReturn("0");
        when(context.getString("hibernate.connection.user")).thenReturn("xinjiang");
        when(context.getString("hibernate.connection.password")).thenReturn("xinjiang");
        when(context.getString("hibernate.dialect")).thenReturn("org.hibernate.dialect.Oracle10gDialect");
        Map<String,String> hibernateProperties = new HashMap<>();
        hibernateProperties.put("hibernate.connection.url", "jdbc:oracle:thin:@10.1.195.102:1521/aidb");
        hibernateProperties.put("hibernate.connection.user", "xinjiang");
        hibernateProperties.put("hibernate.connection.password", "xinjiang");
        hibernateProperties.put("hibernate.dialect", "org.hibernate.dialect.Oracle10gDialect");
        when(context.getSubProperties("hibernate.")).thenReturn(ImmutableMap.copyOf(hibernateProperties));


        mockChannelProcessor = mock(ChannelProcessor.class);
        fixture = new SQLSourceEvent();

        Field field = AbstractSource.class.getDeclaredField("channelProcessor");
        field.setAccessible(true);
        field.set(fixture, mockChannelProcessor);



    }


    @Test
    public void startSqlSourceEvent() throws InterruptedException {
        context = new Context();
        context.put("hibernate.connection.url", "jdbc:oracle:thin:@10.1.195.102:1521/aidb");
        context.put("hibernate.connection.user", "xinjiang");
        context.put("hibernate.connection.password", "xinjiang");
        context.put("hibernate.dialect", "org.hibernate.dialect.Oracle10gDialect");
        context.put("status.file.name", "statusFileName.txt");
        context.put("status.file.path", "d:/");
        //context.put("table", "sys_staff");
        //context.put("columns.to.select", "*");
        context.put("custom.query", "select * from sys_staff order by staff_id asc");
        context.put("custom.query.id", "STAFF_ID");
        context.put("schedule", "0/5 * * * * ?");


        fixture.configure(context);
        fixture.start();

        Thread.sleep(6000L);

        fixture.stop();
    }
}
