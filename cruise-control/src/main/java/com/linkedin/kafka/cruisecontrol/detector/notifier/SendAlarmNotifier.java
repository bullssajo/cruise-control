/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.toDateString;

import java.io.IOException;
import java.util.Map;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendAlarmNotifier extends SelfHealingNotifier {

	private static final Logger LOG = LoggerFactory.getLogger(SendAlarmNotifier.class);
	private static final String SEND_MAIL_URI = "send.mail.uri";
	private static final String SEND_MAIL_RECV_ADDRS = "send.mail.recv";
	private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

	protected String _sendMailUri;
	protected String _sendMailRecv;
	protected String _bootstrapservers;

	public SendAlarmNotifier() {
	}

	public SendAlarmNotifier(Time time) {
		super(time);
	}

	@Override
	public void configure(Map<String, ?> config) {
		super.configure(config);
		_sendMailUri = (String) config.get(SEND_MAIL_URI);
		_sendMailRecv = (String) config.get(SEND_MAIL_RECV_ADDRS);
		_bootstrapservers = (String) config.get(BOOTSTRAP_SERVERS);
	}

	@Override
	public void alert(Object anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
		super.alert(anomaly, autoFixTriggered, selfHealingStartTime, anomalyType);

		if (_sendMailUri == null) {
			LOG.warn("SendMail URI is null, can't send Slack self healing notification");
			return;
		}

		if (_sendMailRecv == null) {
			LOG.warn("SendMail Recv Address is null, can't send Slack self healing notification");
			return;
		}

		String text = String.format("%s detected %s. Self healing %s. %s", anomalyType, anomaly,
				_selfHealingEnabled.get(anomalyType)
						? String.format("start time %s", toDateString(selfHealingStartTime))
						: "is disabled",
				autoFixTriggered ? "Self-healing has been triggered." : "");
		try {
			sendAlarm(text);
		} catch (IOException e) {
			LOG.warn("ERROR sending alert to Slack", e);
		}
	}

	protected void sendAlarm(String text) throws IOException {
		CloseableHttpClient client = HttpClients.createDefault();
		HttpPost httpPost = new HttpPost(_sendMailUri + "?groupoption=true");
		StringBuilder sb = new StringBuilder();
		sb.append("{\"").append("recvAddrs\":\"").append(_sendMailRecv)
				.append("\",\"title\":\"[CruiseControl] Anomaly Detected\",\"content\":\"").append("bootstrap.servers=")
				.append(_bootstrapservers).append("<br/>").append(text).append("\"}");
		StringEntity entity = new StringEntity(sb.toString());
		httpPost.setEntity(entity);
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Content-type", "application/json");
		try {
			client.execute(httpPost);
		} finally {
			client.close();
		}
	}
}
