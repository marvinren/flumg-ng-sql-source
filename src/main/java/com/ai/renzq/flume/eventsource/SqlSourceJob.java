package com.ai.renzq.flume.eventsource;

import com.ai.renzq.flume.metrics.SqlSourceCounter;
import com.ai.renzq.flume.source.HibernateHelper;
import com.ai.renzq.flume.source.SQLSource;
import com.ai.renzq.flume.source.SQLSourceHelper;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.gson.stream.JsonWriter;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.SimpleEvent;
import org.json.simple.JSONObject;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SqlSourceJob is a quartz Job.
 *
 * @author <a href="mailto:renzq@asiainfo.com">Renzq</a>
 *
 */
public class SqlSourceJob implements Job {

    private static final Logger logger = LoggerFactory.getLogger(SQLSourceEvent.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        Context context = (Context) jobExecutionContext.getMergedJobDataMap().get("context");
        SqlSourceCounter sqlSourceCounter = (SqlSourceCounter) jobExecutionContext.getMergedJobDataMap().get("sqlSourceCounter");
        ChannelProcessor channelProcess = (ChannelProcessor) jobExecutionContext.getMergedJobDataMap().get("channelProcess");
        logger.debug("context: {}, sqlSourceCounter: {}" ,context, sqlSourceCounter);

        SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, SqlSourceJob.class.getName());
        HibernateHelper hibernateHelper = new HibernateHelper(sqlSourceHelper);

        hibernateHelper.establishSession();

        sqlSourceCounter.startProcess();
        try(ChannelWriter channelWriter = new ChannelWriter(channelProcess, sqlSourceHelper)) {

            List<Map<String, Object>> result = hibernateHelper.executeQueryJsonRes();
            if (!result.isEmpty())
            {
                for(Map<String, Object> line:result){
                    String id = null;
                    if(!Strings.isNullOrEmpty(sqlSourceHelper.getCustomQueryId())){
                        id = line.get(sqlSourceHelper.getCustomQueryId()).toString();
                    }
                    channelWriter.write(JSONObject.toJSONString(line), id);
                }
                channelWriter.flush();

                sqlSourceCounter.incrementEventCount(result.size());

                sqlSourceHelper.updateStatusFile();
            }
            sqlSourceCounter.endProcess(result.size());

        } catch (InterruptedException e) {
            Throwables.propagate(e);
        } catch (IOException e) {
            Throwables.propagate(e);
        } finally {
            channelProcess.close();
        }

    }


    private class ChannelWriter extends Writer {

        private List<Event> events = new ArrayList<>();
        private SQLSourceHelper sqlSourceHelper;
        private ChannelProcessor channelProcessor;

        public ChannelWriter(ChannelProcessor channelProcessor, SQLSourceHelper sqlSourceHelper){
            this.sqlSourceHelper = sqlSourceHelper;
            this.channelProcessor = channelProcessor;
        }

        public void write(String str, String id) throws IOException {
            write(str.toCharArray(), 0, str.length(), id);
        }

        public void write(char[] cbuf, int off, int len, String id)  throws IOException {
            Event event = new SimpleEvent();
            String s = new String(cbuf);
            event.setBody(s.substring(off, len).getBytes(Charsets.UTF_8));

            Map<String, String> headers;
            headers = new HashMap<String, String>();
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
            headers.put("id", id);

            event.setHeaders(headers);
            events.add(event);

            if (events.size() >= sqlSourceHelper.getBatchSize()) {
                flush();
            }
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {

           this.write(cbuf, off, len, null);
        }

        @Override
        public void flush() throws IOException {
            this.channelProcessor.processEventBatch(events);
            events.clear();
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }
}
