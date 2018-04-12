package com.ai.renzq.flume.eventsource;

import com.ai.renzq.flume.metrics.SqlSourceCounter;
import com.ai.renzq.flume.source.HibernateHelper;
import com.ai.renzq.flume.source.SQLSource;
import com.ai.renzq.flume.source.SQLSourceHelper;
import com.opencsv.CSVWriter;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SQLSourceEvent
 *
 * @author <a href="mailto:renzq@asiainfo.com">Renzq</a>
 *
 */
public class SQLSourceEvent extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(SQLSourceEvent.class);


    private SqlSourceCounter sqlSourceCounter;
    private Context context;
    private Scheduler scheduler;

    @Override
    public void configure(Context context) {


        /* Initialize metric counters */
        sqlSourceCounter = new SqlSourceCounter("SOURCESQL." + this.getName());

        /* Initialize sourcce context */
        this.context = context;

    }

    @Override
    public synchronized void start() {
        SQLSourceHelper sqlSourceHelper = new SQLSourceHelper(context, SqlSourceJob.class.getName());

        /* Start a Quartz Job using the cronTigger for firing the data fetcher */
        try {
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();
            scheduler = schedulerFactory.getScheduler();
            JobDetail jobDetail = JobBuilder.newJob().withIdentity("SqlSourceJob")
                    .ofType(SqlSourceJob.class)
                    .build();
            jobDetail.getJobDataMap().put("context", this.context);
            jobDetail.getJobDataMap().put("sqlSourceCounter", this.sqlSourceCounter);
            jobDetail.getJobDataMap().put("channelProcess", this.getChannelProcessor());
            Trigger cronTrigger = TriggerBuilder.newTrigger()
                    .withIdentity("cron trigger", "test")
                    .withSchedule(
                            CronScheduleBuilder.cronSchedule(sqlSourceHelper.getSchedule())
                    ).build();
            scheduler.scheduleJob(jobDetail, cronTrigger);
            scheduler.start();
        } catch (SchedulerException e) {
            logger.error("start schedule error:" + e.getMessage());
        }

        super.start();
        sqlSourceCounter.start();
    }

    @Override
    public synchronized void stop() {

        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                logger.error("shutdown schedule error:" + e.getMessage(), e);
            }
        }
        super.stop();
        sqlSourceCounter.stop();
    }
}
