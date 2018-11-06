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

import java.util.List;

import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.Validate;

import com.google.api.client.util.Lists;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * The {@code TableSchemaBuilder} class assists in
 * building a BigQuery table schema.
 */
public class TableSchemaBuilder {
  
  private final List<TableFieldSchema> fields = Lists.newArrayList();
  
  public static final String BQ_STRING = "STRING";
  public static final String BQ_BYTES = "BYTES";
  public static final String BQ_INTEGER = "INTEGER";
  public static final String BQ_FLOAT = "FLOAT";
  public static final String BQ_BOOLEAN = "BOOLEAN";
  public static final String BQ_RECORD = "RECORD";
  public static final String BQ_TIMESTAMP = "TIMESTAMP";
  public static final String BQ_DATE = "DATE";
  public static final String BQ_TIME = "TIME";
  public static final String BQ_DATETIME = "DATETIME";
  
  public static final String BQ_MODE_NULLABLE = "NULLABLE";
  public static final String BQ_MODE_REQUIRED = "REQUIRED";
  public static final String BQ_MODE_REPEATED = "REPEATED";
  
  /**
   * Adds a field to the schema.
   * 
   * @param name	The name of field.
   * @param type	The type of the field.
   * @return			The {@code TableSchemaBuilder} instance.
   */
  public TableSchemaBuilder addField(String name, String type) {
    Validate.notNull(name, "The name of a field cannot be null in the TableSchema!");

    fields.add(new TableFieldSchema().setName(name).setType(type));
    return this;
  }
  
  /**
   * Adds a field to the schema.
   * 
   * @param name	The name of field.
   * @param type	The type of the field.
   * @param mode	The mode of the field (NULLABLE, REQUIRED, REPEATED).
   * @return			The {@code TableSchemaBuilder} instance.
   */
  public TableSchemaBuilder addField(String name, String type, String mode) {
    Validate.notNull(name, "The name of a field cannot be null in the TableSchema!");

    fields.add(new TableFieldSchema().setName(name).setType(type).setMode(mode));
    return this;
  }
  
  /**
   * Adds a field to the schema.
   * 
   * @param name	The name of field.
   * @param type	The type of the field.
   * @param mode	The mode of the field (NULLABLE, REQUIRED, REPEATED).
   * @param fields The fields of the record.
   * @return			The {@code TableSchemaBuilder} instance.
   */
  public TableSchemaBuilder addField(String name, String type, String mode, List<TableFieldSchema> fields) {
    Validate.notNull(name, "The name of a field cannot be null in the TableSchema!");

    this.fields.add(new TableFieldSchema().setName(name).setType(type).setMode(mode).setFields(fields));
    return this;
  }
  
  /**
   * Gets the schema from the builder.
   * 
   * @return	The built table schema.
   */
  public TableSchema getSchema() {
    return new TableSchema().setFields(fields);
  }
}
