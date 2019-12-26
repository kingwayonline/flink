package first;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class FirstFlink_stream {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);
		socketTextStream.map((item) -> Tuple2.of(item, 1)).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(0)
		.timeWindow(Time.seconds(5),Time.seconds(2))
		.sum(1)
		.print();
		
		env.execute();

//		DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);
//		socketTextStream.flatMap((String i,Collector<Tuple2<String,Integer>> c)->{
//			String[] split = i.split(",");
//			for (String string : split) {
//				c.collect(Tuple2.of(string, 1));
//			}
//		}).returns(Types.TUPLE(Types.STRING,Types.INT)) .keyBy(0)
//		  .timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);
//		
//		env.execute("FirstFlink_stream");

//		DataStreamSource<Long> setParallelism = env.addSource(new SourceFuction()).setParallelism(1);
//		SplitStream<Long> split = setParallelism.split(new OutputSelector<Long>() {
//			
//			@Override
//			public Iterable<String> select(Long value) {
//				
//				List<String> output = new ArrayList<String>();
//				if (value % 2 == 0) {
//		            output.add("even");
//		        }
//		        else {
//		            output.add("odd");
//		        }
//				System.out.println(output.get(0));
//		        return output;
//			}
//		});
//		split.select("even").print();
//		env.execute("FirstFlink_stream");

//		DataStreamSource<Long> setParallelism1 = env.addSource(new SourceFuction()).setParallelism(1);
//		DataStreamSource<Long> setParallelism2 = env.addSource(new SourceFuction()).setParallelism(1);
//		DataStream<Long> union = setParallelism1.union(setParallelism2);
//		union.print();
//		env.execute("FirstFlink_stream");

//		env.addSource(new SourceFuction()).setParallelism(1)
//		   .map((i) -> {
//			   System.out.println("map: "+i);
//			   return i;
//		   })
//		   .filter((i) -> i % 2 == 0)
//		   .print();
//		env.execute("FirstFlink_stream");

//		env.addSource(new SourceFuction()).setParallelism(1).print().setParallelism(1);
//		env.execute("FirstFlink_stream");

//		DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);
//		socketTextStream.map((i)->i.toLowerCase()).print();
//		env.execute("FirstFlink_stream");

//		DataStreamSource<String> text = env.socketTextStream("localhost", 9999);
//		text.flatMap((String i, Collector<Tuple2<String, Integer>> collector) -> {
//			collector.collect(new Tuple2<String, Integer>(i, 1));
//		}).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print()
//				.setParallelism(1);
//		env.execute("abc");
	}
}
