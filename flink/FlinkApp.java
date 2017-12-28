/**
 *
 */
package de.dfki.mlt.wre.flink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Aydan Rende, DFKI
 *
 */
public class FlinkApp {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment().setParallelism(2);
		DataStream<Tuple3<String, String, String>> stream = env.addSource(
				new SentenceDatasource()).rescale();

		stream.print();
		env.execute("Flink-App");
	}
}
