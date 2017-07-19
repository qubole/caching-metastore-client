/*
 *  Copyright (c) 2016 Liwen Fan

 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:

 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.

 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package com.qubole.utility;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

/**
 * Created by sakshibansal on 28/04/17.
 */

/**
 * @param <T> Type: Deserialization type
 */
public class JdkSerializer<T> implements Serializer {

  @Override
  public byte[] serialize(Object object) {
    if (object instanceof String) {
      return ((String) object).getBytes();
    }
    return SerializationUtils.serialize((Serializable) object);
  }

  @Override
  public T deserialize(byte[] objectData) {
    return (T) SerializationUtils.deserialize(objectData);
  }
}
