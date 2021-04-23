/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.jms.source.utils;

import com.google.gson.Gson;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;

import static org.mockito.Mockito.when;

public class BytesMessageTestUtils {

  private static final Map<String, Object> DUMMY_DATA =
    Collections.unmodifiableMap(new LinkedHashMap<String, Object>() {{
      put("string_body", "Hello!");
      put("double_body", 1D);
      put("float_body", 1F);
      put("int_body", 1);
      put("long_body", 1L);
      put("boolean_body", true);
      put("byte_body", (byte) 1);
      put("bytes_body", -1);
    }});

  public static String getBytesMessageAsJsonString() {
    return new Gson().toJson(DUMMY_DATA).toString().replace("-1", "[]");
  }

  public static void mockBytesMessage(BytesMessage bytesMessage) throws JMSException {
    when(bytesMessage.readBoolean()).thenReturn((boolean) DUMMY_DATA.get("boolean_body"));
    when(bytesMessage.readByte()).thenReturn((byte) DUMMY_DATA.get("byte_body"));
    when(bytesMessage.readInt()).thenReturn((int) DUMMY_DATA.get("int_body"));
    when(bytesMessage.readLong()).thenReturn((long) DUMMY_DATA.get("long_body"));
    when(bytesMessage.readFloat()).thenReturn((float) DUMMY_DATA.get("float_body"));
    when(bytesMessage.readDouble()).thenReturn((double) DUMMY_DATA.get("double_body"));
    when(bytesMessage.readUTF()).thenReturn((String) DUMMY_DATA.get("string_body"));
    when(bytesMessage.readBytes(new byte[8096])).thenReturn((int) DUMMY_DATA.get("bytes_body"));
    when(bytesMessage.getBodyLength()).thenReturn(1L);
  }
}
