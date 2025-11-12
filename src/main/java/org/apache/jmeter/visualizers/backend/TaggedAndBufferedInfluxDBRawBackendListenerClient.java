package org.apache.jmeter.visualizers.backend.influxdb;

import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.visualizers.backend.influxdb.InfluxDBRawBackendListenerClient;
import org.apache.jmeter.visualizers.backend.influxdb.AbstractInfluxdbMetricsSender;
import org.apache.jmeter.config.Arguments;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TaggedAndBufferedInfluxDBRawBackendListenerClient extends InfluxDBRawBackendListenerClient {

    private final Map<String, String> dynamicTags = new HashMap<>();
    private static final Object LOCK = new Object();
    private static final Logger log = LoggingManager.getLoggerForClass();
    private final AtomicLong lastWriteTime = new AtomicLong(0);
    private int metricCount = 0; // Variable to track the number of metrics

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParams = new Arguments();
        defaultParams.addArgument("influxdbMetricsSender", "org.apache.jmeter.visualizers.backend.influxdb.HttpMetricsSender");
        defaultParams.addArgument("influxdbUrl", "http://host_to_change:8086/write?db=jmeter");
        defaultParams.addArgument("influxdbToken", "");
        defaultParams.addArgument("measurement", "jmeter");
        defaultParams.addArgument("batchSeconds", "5");

        return defaultParams;
    }

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        super.setupTest(context);
        context.getParameterNamesIterator().forEachRemaining(paramName -> {
            if (paramName.startsWith("TAG_")) {
                String tagName = paramName.substring(4); // Remove "_TAG" prefix
                String tagValue = context.getParameter(paramName);
                dynamicTags.put(tagName, tagValue);
            }
        });
    }

    @Override
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        synchronized (LOCK) {
            for (SampleResult sampleResult : sampleResults) {
                addMetricFromSampleResult(sampleResult);
            }

            // Check if it's time to send metrics
            long currentTime = System.currentTimeMillis();
            long batchMillis = Long.parseLong(context.getParameter("batchSeconds")) * 1000L;
            if (currentTime - lastWriteTime.get() >= batchMillis) {
                log.info("Sending " + metricCount + " metrics to InfluxDB.");
                getInfluxDBMetricsManager().writeAndSendMetrics();
                metricCount = 0;
                lastWriteTime.set(currentTime);
            }
        }
    }

    private void addMetricFromSampleResult(SampleResult sampleResult) {
        try {
            String tags = "," + getTags(sampleResult);
            String fields = getFields(sampleResult);
            long timestamp = sampleResult.getTimeStamp();
            getInfluxDBMetricsManager().addMetric(getMeasurement(), tags, fields, timestamp);
            metricCount++; // Increment the count whenever a metric is added
        } catch (IllegalArgumentException e) {
            log.warn("Skipping sample result due to error: " + e.getMessage());
        }
    }

    private String getTags(SampleResult sampleResult) throws IllegalArgumentException {
        boolean isError = sampleResult.getErrorCount() != 0;
        String status = isError ? "ko" : "ok";
        String label = StringUtils.strip(sampleResult.getSampleLabel(), "\" ");
        String transaction = AbstractInfluxdbMetricsSender.tagToStringValue(label);
        String threadName = deleteWhitespace(sampleResult.getThreadName());

        if (threadName == null || threadName.isEmpty()) {
            throw new IllegalArgumentException("Thread name is null or empty for sample result: " + sampleResult.getSampleLabel());
        }

        StringBuilder tagsBuilder = new StringBuilder();

        for (String tagKey : dynamicTags.keySet()) {
            tagsBuilder.append(tagKey).append("=").append(dynamicTags.get(tagKey)).append(",");
        }

        tagsBuilder.append("status=").append(status);
        tagsBuilder.append(",transaction=").append(transaction);
        tagsBuilder.append(",threadName=").append(threadName);

        return tagsBuilder.toString();
    }

    private String getFields(SampleResult sampleResult) {
        StringBuilder fieldsBuilder = new StringBuilder();
        long duration = sampleResult.getTime();
        long latency = sampleResult.getLatency();
        long connectTime = sampleResult.getConnectTime();

        fieldsBuilder.append("duration=").append(duration);
        fieldsBuilder.append(",ttfb=").append(latency);
        fieldsBuilder.append(",connectTime=").append(connectTime);
        return fieldsBuilder.toString();
    }

    private static String deleteWhitespace(String str) {
        if (str == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(str.length());
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (!Character.isWhitespace(c)) {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}