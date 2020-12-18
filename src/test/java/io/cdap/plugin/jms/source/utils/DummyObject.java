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

import java.io.Serializable;
import java.util.Objects;

public class DummyObject implements Serializable {
  String dummyStr;
  int dummyInt;

  public DummyObject(String dummyStr, int dummyInt) {
    this.dummyStr = dummyStr;
    this.dummyInt = dummyInt;
  }

  public String getDummyStr() {
    return dummyStr;
  }

  public void setDummyStr(String dummyStr) {
    this.dummyStr = dummyStr;
  }

  public int getDummyInt() {
    return dummyInt;
  }

  public void setDummyInt(int dummyInt) {
    this.dummyInt = dummyInt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DummyObject that = (DummyObject) o;
    return dummyInt == that.dummyInt && Objects.equals(dummyStr, that.dummyStr);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dummyStr, dummyInt);
  }
}
