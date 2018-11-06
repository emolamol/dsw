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

package com.google.cloud.pso;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.pso.dofn.PubSubMessageToTableRow;
import com.google.cloud.pso.schema.BigQueryDeadletterSchema;
import com.google.common.collect.Lists;

/**
 * The {@link LoyaltyPipeline} pipeline ingests POS Log data into BigQuery for
 * analysis. When a new event comes in, it is first deduped by Pub/Sub using the
 * id attribute on the Pub/Sub message. Then the JSON is parsed and attributes
 * are extracted and set into {@link LoyaltyInfo}. Next Loyalty Engine is called
 * and JSON response from Loyalty Engine is parsed. After combining data from
 * POSLog and Loyalty Engine, records are written to sample BigQuery tables -
 * dsw_loyalty_event_history and dsw_loyalty_tier_history. If any record fails
 * during parsing of POSLog JSON, POSLog Json will be placed in a sideOutput and
 * sent downstream to BigQuery to be inserted into a deadletter.
 * 
 * Example Usage: mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.pso.pipeline.SensorStoreIngest \
 * -Dexec.cleanupDaemonThreads=false \ -Dexec.args=" \ 
 * --project=${PROJECT} \
 * --stagingLocation=${GCS_PIPELINE_LOCATION}/staging \
 * --tempLocation=${GCS_PIPELINE_LOCATION}/temp \ 
 * --runner=${RUNNER} \
 * --streaming=true \ 
 * --region=us-east1 \ 
 * --zone=us-east1-d \
 * --workerMachineType=n1-standard-2 \ 
 * --numWorkers=20 \ 
 * --maxNumWorkers=150 \
 * --workerDiskType=compute.googleapis.com/projects//zones//diskTypes/pd-ssd \
 * --diskSizeGb=100 \
 * --subscription=projects/${PROJECT}/subscriptions/dsw_loyalty_subscription \
 * --loyaltyInstanceId=demos"
 */
public class LoyaltyPipeline {

	/**
	 * The logger to output status messages to. Be judicious with logging as it can
	 * cause major pipeline slow downs.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(LoyaltyPipeline.class);

	/**
	 * The attribute which will be used for de-duping.
	 */
	private static final String PUB_SUB_ID_ATTR = "id";
	
	/**
	 * The tag for Failed records.
	 */
	private static final TupleTag<PubsubMessage> DEADLETTER_TAG = new TupleTag<>();

	/**
	 * The tag for successful Loyalty POSLog JSON.
	 */
	private static final TupleTag<LoyaltyInfo> LOYALTY_INFO_TAG = new TupleTag<>();

	/**
	 * The format which dates will be saved & retrieved.
	 */
	private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	/**
	 * Dummy hard coded response from Loyalty Engine
	 * 
	 * {  
		   "Loyalty":{  
		      "party_id":"34",
		      "loyalty_acct_id":"7373",
		      "gender":"Female",
		      "loyalty_member_status":"Active",
		      "loyalty_member_current_tier":"Platinum",
		      "loyalty_member_previous_tier":"Gold",
		      "loyalty_event_type":"Order_Placed"
		   }
		}
	 */
	private static String dummyLoyaltyEngineResponse = "{\"Loyalty\":{\"party_id\":\"34\",\"loyalty_acct_id\":\"7373\",\"gender\":\"Female\",\"loyalty_member_status\":\"Active\",\"loyalty_member_current_tier\":\"Platinum\",\"loyalty_member_previous_tier\":\"Gold\",\"loyalty_event_type\":\"Order_Placed\"}}";

	/**
	 * Options supported by the pipeline.
	 * 
	 * <p>
	 * Inherits standard configuration options.
	 * </p>
	 */
	public static interface MyOptions extends DataflowPipelineOptions, StreamingOptions {
		@Description("The Cloud Pub/Sub subscription to read from.")
		@Required
		@Default.String("projects/data-analytics-pocs/subscriptions/dsw_loyalty_subscription")
		String getSubscription();

		void setSubscription(String value);

		@Description("The instance id which holds the event data.")
		@Required
		@Default.String("demos")
		String getLoyaltyInstanceId();

		void setLoyaltyInstanceId(String value);

	}

	/**
	 * Main entry point for executing the pipeline.
	 * 
	 * @param args
	 *            The command-line arguments to the pipeline.
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		//set streaming to true
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);
		
		String table_dswLoyalty_event_history = options.getProject() + ":" + options.getLoyaltyInstanceId()
				+ ".dsw_loyalty_event_history";
		String table_dsw_loyalty_tier_history = options.getProject() + ":" + options.getLoyaltyInstanceId()
				+ ".dsw_loyalty_tier_history";
		String table_dsw_deadletter_table = options.getProject() + ":" + options.getLoyaltyInstanceId()
				+ ".deadletter_table";

		// Build the table schema for the output table -
		// dsw_loyalty_event_history
		List<TableFieldSchema> table_dswLoyalty_event_history_fields = new ArrayList<>();
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("retailstoreid").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("workstationid").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("businessdaydate").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("operatorname").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("itemcount").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("tender_total").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("tender_type").setType("STRING"));
		table_dswLoyalty_event_history_fields
				.add(new TableFieldSchema().setName("transaction_discount_amount").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("taxtotal").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("barcode").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("processedTime").setType("TIMESTAMP"));
		
		//schema for nested repeated fields for lineitems
		ArrayList<TableFieldSchema> lineItemFields = Lists.newArrayList(
				new TableFieldSchema().setName("sequence_number").setType("STRING"),
				new TableFieldSchema().setName("item_type").setType("STRING"),
				new TableFieldSchema().setName("unit_list_price").setType("STRING"),
				new TableFieldSchema().setName("regular_sales_unit_price").setType("STRING"),
				new TableFieldSchema().setName("extended_discount_amount").setType("STRING"),
				new TableFieldSchema().setName("quantity").setType("STRING"));
		table_dswLoyalty_event_history_fields.add(new TableFieldSchema().setName("line_items").setType("STRUCT")
				.setMode("REPEATED").setFields(lineItemFields));
		TableSchema table_dswLoyalty_event_history_fields_schema = new TableSchema()
				.setFields(table_dswLoyalty_event_history_fields);

		// Build the table schema for the output table - table_dsw_loyalty_tier_history
		List<TableFieldSchema> table_dsw_loyalty_tier_history_fields = new ArrayList<>();
		table_dsw_loyalty_tier_history_fields.add(new TableFieldSchema().setName("party_id").setType("STRING"));
		table_dsw_loyalty_tier_history_fields.add(new TableFieldSchema().setName("loyalty_acct_id").setType("STRING"));
		table_dsw_loyalty_tier_history_fields
				.add(new TableFieldSchema().setName("loyalty_member_status").setType("STRING"));
		table_dsw_loyalty_tier_history_fields
				.add(new TableFieldSchema().setName("loyalty_member_current_tier").setType("STRING"));
		table_dsw_loyalty_tier_history_fields
				.add(new TableFieldSchema().setName("loyalty_member_previous_tier").setType("STRING"));
		table_dsw_loyalty_tier_history_fields
				.add(new TableFieldSchema().setName("loyalty_event_type").setType("STRING"));
		table_dsw_loyalty_tier_history_fields
				.add(new TableFieldSchema().setName("loyalty_event_time").setType("TIMESTAMP"));
		TableSchema table_dsw_loyalty_tier_history_schema = new TableSchema()
				.setFields(table_dsw_loyalty_tier_history_fields);
		
		/**
		 * Steps:
		 * 1) Read POSLog from PubSub
		 * 2) Parse POSLog JSON and extract attributes
		 * 3) Call Loyalty Engine and parse JSON response
		 * 4) Write successful records to Loyalty history and tier table
		 * 5) Write failed records to BigQuery deadletter table
		 */
		PCollectionTuple parsedPOSLTransaction = p //
				.apply("PosLogFromPubSub",
						PubsubIO.readMessagesWithAttributes().withIdAttribute(PUB_SUB_ID_ATTR)
								.fromSubscription(options.getSubscription())) //
				.apply("ExtractPosLogData", ParDo.of(new DoFn<PubsubMessage, LoyaltyInfo>() {
					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						PubsubMessage pubSubMessage = c.element();
						LOG.info("incoming message -----" + pubSubMessage.getAttributeMap().toString());
						try {
							String prosLogJsonline = new String(pubSubMessage.getPayload(), "UTF-8");
							LOG.info(prosLogJsonline);
							c.output(LoyaltyInfo.newLoyaltyInfo(prosLogJsonline));
						} catch (Exception e) {
							LOG.error("Error parsing message. Outputting to deadletter: {}", pubSubMessage.toString(),
									e);
							c.output(DEADLETTER_TAG, pubSubMessage);
						}
					}
				}).withOutputTags(LOYALTY_INFO_TAG, TupleTagList.of(DEADLETTER_TAG)));
		//Call Loyalty Engine and parse JSON response
		PCollection<LoyaltyInfo> parsedLoyaltyEngineOutput = parsedPOSLTransaction.get(LOYALTY_INFO_TAG)
				.setCoder(AvroCoder.of(LoyaltyInfo.class))
				.apply("CallLoyaltyEngine", ParDo.of(new DoFn<LoyaltyInfo, LoyaltyInfo>() {
					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						LoyaltyInfo info = c.element();
						info = LoyaltyInfo.setAdditionalLoyaltyInfo(info, dummyLoyaltyEngineResponse);
						c.output(info);
					}
				}));
		
		// Write failed records to BigQuery deadletter table
		TableSchema deadletterSchema = BigQueryDeadletterSchema.getSchema();
		parsedPOSLTransaction.get(DEADLETTER_TAG).setCoder(PubsubMessageWithAttributesCoder.of())
				.apply("FailedToTableRows", ParDo.of(new PubSubMessageToTableRow())).apply("WriteDeadletterToBigQuery",
						BigQueryIO.writeTableRows().to(table_dsw_deadletter_table).withSchema(deadletterSchema)
								.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
								.withWriteDisposition(WriteDisposition.WRITE_APPEND));
		
		//Write successful records to Loyalty Loyalty History Table table
		parsedLoyaltyEngineOutput.apply("EventHistoryRow", ParDo.of(new DoFn<LoyaltyInfo, TableRow>() {
			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
				TableRow loyalty_event_history_row = new TableRow();

				LoyaltyInfo info = c.element();
				loyalty_event_history_row.set("retailstoreid", info.getRetailStoreID());
				loyalty_event_history_row.set("workstationid", info.getWorkstationID());
				loyalty_event_history_row.set("businessdaydate", info.getBusinessDayDate());
				loyalty_event_history_row.set("operatorname", info.getOperatorName());
				loyalty_event_history_row.set("itemcount", info.getItemCount());
				loyalty_event_history_row.set("tender_total", info.getTender_Total());
				loyalty_event_history_row.set("tender_type", info.getTender_type());
				loyalty_event_history_row.set("transaction_discount_amount", info.getTransaction_Discount_Amount());
				loyalty_event_history_row.set("taxtotal", info.getTaxTotal());
				loyalty_event_history_row.set("barcode", info.getBarcode());

				List<TableRow> lineItemsRows = new ArrayList<TableRow>();
				for (Map<String, String> lineItemMap : info.lineItems) {
					TableRow newLineItem = new TableRow();
					newLineItem.set("sequence_number", lineItemMap.get("sequence_number"));
					newLineItem.set("item_type", lineItemMap.get("item_type"));
					newLineItem.set("unit_list_price", lineItemMap.get("unit_list_price"));
					newLineItem.set("regular_sales_unit_price", lineItemMap.get("regular_sales_unit_price"));
					newLineItem.set("extended_discount_amount", lineItemMap.get("extended_discount_amount"));
					newLineItem.set("quantity", lineItemMap.get("quantity"));
					lineItemsRows.add(newLineItem);
				}
				loyalty_event_history_row.set("line_items", lineItemsRows);
				loyalty_event_history_row.set("processedTime", LocalDateTime.now().format(dateTimeFormatter));
				c.output(loyalty_event_history_row);
			}
		})) //
				.apply("EventHistoryTable", BigQueryIO.writeTableRows().to(table_dswLoyalty_event_history)//
						.withSchema(table_dswLoyalty_event_history_fields_schema)//
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		
		//Write successful records to Loyalty Loyalty Tier Table table
		parsedLoyaltyEngineOutput.apply("TierHistoryRow", ParDo.of(new DoFn<LoyaltyInfo, TableRow>() {
			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
				TableRow dsw_loyalty_tier_history_row = new TableRow();

				LoyaltyInfo info = c.element();
				dsw_loyalty_tier_history_row.set("party_id", info.getParty_id());
				dsw_loyalty_tier_history_row.set("loyalty_acct_id", info.getLoyalty_acct_id());
				dsw_loyalty_tier_history_row.set("loyalty_member_status", info.getLoyalty_member_status());
				dsw_loyalty_tier_history_row.set("loyalty_member_current_tier", info.getLoyalty_member_current_tier());
				dsw_loyalty_tier_history_row.set("loyalty_member_previous_tier",
						info.getLoyalty_member_previous_tier());
				dsw_loyalty_tier_history_row.set("loyalty_event_type", info.getLoyalty_event_type());
				dsw_loyalty_tier_history_row.set("loyalty_event_time", info.getLoyalty_event_time());
				c.output(dsw_loyalty_tier_history_row);
			}
		})) //
				.apply("TierHistoryTable", BigQueryIO.writeTableRows().to(table_dsw_loyalty_tier_history)//
						.withSchema(table_dsw_loyalty_tier_history_schema)//
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		p.run();
	}
}
