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
import java.util.Arrays;

public class TaggedAndBufferedInfluxDBRawBackendListenerClient extends InfluxDBRawBackendListenerClient {

    private final Map<String, String> dynamicTags = new HashMap<>();
    private static final Object LOCK = new Object();
    private static final Logger log = LoggingManager.getLoggerForClass();
    private final AtomicLong lastWriteTime = new AtomicLong(0);
    private int metricCount = 0; 
    private boolean verboseLogging = false;
    private String includedFields = "duration,ttfb,connectTime";
    private String includedTags = "status,transaction,threadName"; // Default included tags

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParams = new Arguments();
        defaultParams.addArgument("influxdbMetricsSender", "org.apache.jmeter.visualizers.backend.influxdb.HttpMetricsSender");
        defaultParams.addArgument("influxdbUrl", "http://host_to_change:8086/write?db=jmeter");
        defaultParams.addArgument("influxdbToken", "");
        defaultParams.addArgument("measurement", "jmeter");
        defaultParams.addArgument("batchSeconds", "5");
        defaultParams.addArgument("verboseLogging", "false");
        defaultParams.addArgument("includedFields", "duration,ttfb,connectTime");
        defaultParams.addArgument("includedTags", "status,transaction,threadName");
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
        verboseLogging = Boolean.parseBoolean(context.getParameter("verboseLogging", "false"));
        includedFields = context.getParameter("includedFields", "duration,ttfb,connectTime");
        includedTags = context.getParameter("includedTags", "status,transaction,threadName");
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
                log.info("Sending " + metricCount + " samples to InfluxDB.");
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
        String label = StringUtils.strip(sampleResult.getSampleLabel(), " ");
        String transaction = AbstractInfluxdbMetricsSender.tagToStringValue(label);
        String threadName = deleteWhitespace(sampleResult.getThreadName());

        if (threadName == null || threadName.isEmpty()) {
            throw new IllegalArgumentException("Thread name is null or empty for sample result: " + sampleResult.getSampleLabel());
        }

        StringBuilder tagsBuilder = new StringBuilder();

        // Add dynamic tags
        for (String tagKey : dynamicTags.keySet()) {
            tagsBuilder.append(tagKey).append("=").append(dynamicTags.get(tagKey)).append(",");
        }

        // Add included tags based on the includedTags parameter
        List<String> includedTagsList = Arrays.asList(includedTags.split(","));
        if (includedTagsList.contains("status")) {
            tagsBuilder.append("status=").append(status).append(",");
        }
        if (includedTagsList.contains("transaction")) {
            tagsBuilder.append("transaction=").append(transaction).append(",");
        }
        if (includedTagsList.contains("threadName")) {
            tagsBuilder.append("threadName=").append(threadName).append(",");
        }

        // Remove trailing comma if present
        if (tagsBuilder.length() > 0) {
            tagsBuilder.setLength(tagsBuilder.length() - 1);
        }

         if (verboseLogging) {
            log.info("Generated tags - " + tagsBuilder.toString());
        }

        return tagsBuilder.toString();
    }

    private String getFields(SampleResult sampleResult) {
        StringBuilder fieldsBuilder = new StringBuilder();
        long duration = sampleResult.getTime();
        long latency = sampleResult.getLatency();
        long connectTime = sampleResult.getConnectTime();

        List<String> includedFieldsList = Arrays.asList(this.includedFields.split(","));
        if (includedFieldsList.contains("duration")) {
            fieldsBuilder.append("duration=").append(duration).append(",");
        }
        if (includedFieldsList.contains("ttfb")) {
            fieldsBuilder.append("ttfb=").append(latency).append(",");
        }
        if (includedFieldsList.contains("connectTime")) {
            fieldsBuilder.append("connectTime=").append(connectTime).append(",");
        }

        // Remove trailing comma if present
        if (fieldsBuilder.length() > 0) {
            fieldsBuilder.setLength(fieldsBuilder.length() - 1);
        }

        if (verboseLogging) {
            log.info("Generated Fields - " + fieldsBuilder.toString());
        }

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