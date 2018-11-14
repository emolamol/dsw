/**
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.instashop.cloud.online.dofn;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Lists;

/**
 * Converts the PubSub message to TableRow.
 */
@SuppressWarnings("serial")
public class PubSubMessageToTableRow extends DoFn<PubsubMessage, TableRow> {
	/**
	 * The logger to output status messages to. Be judicious with logging as
	 * it can cause major pipeline slow downs. 
	 */
	private static final Logger LOG = LoggerFactory.getLogger(PubSubMessageToTableRow.class);
	
	/**
	 * Converts the pubsub message to a table row with the format of
	 * {attributes: [...], payloadString: ..., payloadBytes: ...}
	 * 
	 * @param context	The current process context containing the record in-flight.
	 */
	@ProcessElement
	public void processElement(ProcessContext context) {
		PubsubMessage message = context.element();
		
		// Build the attribute map
		List<TableRow> attributes = Lists.newArrayList();
		message.getAttributeMap()
			.entrySet()
			.forEach(entry -> {
				attributes.add(new TableRow()
						.set("key", entry.getKey())
						.set("value", entry.getValue()));
			});
		LOG.info(message.getAttributeMap().entrySet().toString());
		// Build the table row.
		TableRow row = new TableRow()
				.set("attributes", attributes)
				.set("payloadString", bytesToStringUtf8(message.getPayload()))
				.set("payloadBytes", message.getPayload());
		
		context.output(row);
	}
	
	/**
	 * Converts a byte array to a string with a UTF-8
	 * encoding.
	 * @param bytes	The payload to convert.
	 * @return	The UTF-8 string.
	 */
	private static String bytesToStringUtf8(byte[] bytes) {
		String value = null;

		try {
			value = new String(bytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.error("Error converting bytes to string: {}", bytes);
		}
		
		return value;
	}
}
