/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import java.text.SimpleDateFormat;
import java.util.Date;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JsonMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * from JSON data and partitions data by date.
 */
public class DateJsonMessageParser extends TimestampedMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(DateJsonMessageParser.class);
    public DateJsonMessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        if (jsonObject != null) {
            Object fieldValue = jsonObject.get(mConfig.getMessageTimestampName());
	    Object inputPattern = mConfig.getMessageTimestampInputPattern();
	    LOG.debug("mConfig.getMessageTimestampName()={}, fieldValue={}, inputPattern={}", mConfig.getMessageTimestampName(), fieldValue, inputPattern);
            if (fieldValue != null && inputPattern != null) {
		try {
        	    SimpleDateFormat inputFormatter = new SimpleDateFormat(inputPattern.toString());
        	    Date dateFormat = inputFormatter.parse(fieldValue.toString());
		    LOG.debug("dateFormat={}", dateFormat);
        	    return toMillis(dateFormat.getTime());
                } catch (Exception e) {
		    throw new RuntimeException(e);
		}
            }
        }
        return 0;
    }

}
