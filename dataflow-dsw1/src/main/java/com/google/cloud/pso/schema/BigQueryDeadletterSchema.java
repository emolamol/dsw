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

package com.google.cloud.pso.schema;

import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.Validate;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;

/**
 * The {@code BigQueryDeadletterSchema} holds the schema
 * for the deadletter_events table in BigQuery.
 */
public class BigQueryDeadletterSchema {
	
	/**
	 * The table name of the deadletter events table.
	 */
	private static final String baseName = ":iot_demo.deadletter_events";
	
  /**
   * The schema for the deadletter_events table.
   */
  private static final TableSchema SCHEMA = new TableSchemaBuilder()
      .addField("attributes", TableSchemaBuilder.BQ_RECORD, TableSchemaBuilder.BQ_MODE_REPEATED,
      		Lists.newArrayList(
      				new TableFieldSchema().setName("key").setType("STRING"),
      				new TableFieldSchema().setName("value").setType("STRING")))
      .addField("payloadString", TableSchemaBuilder.BQ_STRING)
      .addField("payloadBytes", TableSchemaBuilder.BQ_BYTES)
      .getSchema();
  
  /**
   * Retrieves the table name.
   * 
   * @return The table name.
   */
  public static String getTableName(String projectId) {
  	Validate.notEmpty(projectId, 
  			"The project id must be a non-empty value to generate a table name.");
  	return projectId + baseName;
  }
  
  /**
   * Retrieves the table schema.
   * 
   * @return	The table schema.
   */
  public static TableSchema getSchema() {
  	return SCHEMA;
  }
}
