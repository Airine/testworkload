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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Fake Workload for CS6211 Project
 */
public class FakeWorkLoad {

//	public static int scenario;//

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		int runtime 	= params.getInt("runtime", 30);
		int nKeys		= params.getInt("nKeys", 16);
		int inputRate	= params.getInt("inputRate", 4);
		int serviceRate	= params.getInt("serviceRate", 5);
		int outputRate	= params.getInt("outputRate", 5);
		int wordSize	= params.getInt("wordSize", 512);

		DataStream<Tuple3<Integer, Long, String>> largeWords = env
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

		DataStream<Tuple3<Integer, Long, String>> counts = largeWords
				.keyBy(0)
				.flatMap(new CounterMap(serviceRate))
				.setParallelism(1)
				.name("Count")
				.uid("OperatorB")
				.disableChaining();

		DataStream<Double> latency = counts.keyBy(0)
				.flatMap(new DummySink(outputRate))
				.setParallelism(1)
				.name("Sink: Sink")
				.uid("OperatorC")
				.disableChaining();

		latency.countWindowAll(10)
				.sum(0)
				.print();

		env.disableOperatorChaining();

		env.execute();
	}
}
