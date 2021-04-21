/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package testworkload;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Fake Workload for CS6211 Project
 */
public class FakeWorkLoad {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		int runtime 	= params.getInt("runtime", 10);
		int nKeys		= params.getInt("nKeys", 100);
		int inputRate	= params.getInt("inputRate", 100);
		int serviceRate	= params.getInt("serviceRate", 200);
		int outputRate	= params.getInt("outputRate", 200);
		int wordSize	= params.getInt("wordSize", 32);

		DataStream<Tuple2<Integer, String>> largeWords = env
				.addSource(new LargeWordsGenerator(
						runtime,
						nKeys,
						inputRate,
						wordSize
				))
				.setParallelism(1) // source operator always have 1 parallelism
				.name("Source")
				.uid("OperatorA")
				.disableChaining();

		DataStream<Tuple2<Integer, String>> counts = largeWords
				.keyBy(0)
				.flatMap(new CounterMap(serviceRate))
				.setParallelism(1)
				.name("Count")
				.uid("OperatorB")
				.disableChaining();

		counts.keyBy(0)
				.addSink(new DummySink(outputRate))
				.setParallelism(1)
				.name("Sink")
				.uid("OperatorC")
				.disableChaining();

		env.disableOperatorChaining();

		env.execute();
	}
}
