package first;

import java.io.File;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

public class FirstFlink_patch {

	public static void main(String[] args) throws Exception {
	        
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		List<String> list = Arrays.asList(
				"java",
				"php",
				"c",
				"spark1"
				);

//		env.registerCachedFile("file:\\D:\\input\\1.txt", "test-file");
//		env.fromCollection(list).map(new RichMapFunction<String, String>() {

//			@Override
//			public void open(Configuration parameters) throws Exception {
//				super.open(parameters);
//				 File file = getRuntimeContext().getDistributedCache().getFile("test-file");
//				 List<String> readLines = FileUtils.readLines(file);
//				 for (String string : readLines) {
//					 String[] split = string.split(" ");
//					 for (String string2 : split) {
//						System.out.println(string2);
//					}
//				}
//			}
//			
//			@Override
//			public String map(String value) throws Exception {
//				return value;
//			}
//		}).setParallelism(1).print();
		
//		List<Tuple2<String,String>> list2 = Arrays.asList(
//				Tuple2.of("1", "天津"),
//				Tuple2.of("2", "北京"),
//				Tuple2.of("3", "上海"),
//				Tuple2.of("8", "成都")
//				);
//		
		
//		DataSet<String> readTextFile = env.fromCollection(list);
//		DataSet<String> info = readTextFile.map(new RichMapFunction<String, String>() {
//
//			LongCounter counter = new LongCounter();
//
//			@Override
//			public void open(Configuration parameters) throws Exception {
//				super.open(parameters);
//				//getRuntimeContext().addAccumulator("ele", counter);
//				RuntimeContext runtimeContext = getRuntimeContext();
//				runtimeContext.addAccumulator("elea", counter);
//			}
//
//			@Override
//			public String map(String value) throws Exception {
//				counter.add(1);
//				return value;
//			}
//		});
//		info.writeAsText("file:\\D:\\input\\counter.text",WriteMode.OVERWRITE);
//		JobExecutionResult execute = env.execute("FirstFlink_patch");
//		long num = execute.getAccumulatorResult("elea");
//		System.out.println(num);
		
//		readTextFile.writeAsText("file:\\D:\\input\\",WriteMode.OVERWRITE);
//		DataSet<Tuple2<String,String>> readTextFile2 = env.fromCollection(list2);
//		env.execute("FirstFlink_patch");
		
//		readTextFile.rightOuterJoin(readTextFile2).where(0).equalTo(0)
//		.with((x,y)->{
//			if (x==null) {
//				return  Tuple3.of(y.f0, "-", y.f1);
//			}else {
//				return  Tuple3.of(y.f0, x.f1, y.f1);
//			}
//		}).returns(Types.TUPLE(Types.STRING,Types.STRING,Types.STRING))
//		.print();
		
		
//		readTextFile.join(readTextFile2).where(0).equalTo(0)
//		.with((x,y)->Tuple3.of(x.f0, x.f1, y.f1)).returns(Types.TUPLE(Types.STRING,Types.STRING,Types.STRING))
//		.print();
		
//		readTextFile.join(readTextFile2).where(0).equalTo(0)
//		.with((x,y)->Tuple3.of(x.f0,x.f1,y.f1)).returns(Types.TUPLE(Types.STRING,Types.STRING,Types.STRING))
//		.print();
		
		 
//		readTextFile.distinct().print();
		
//		Configuration parameters = new Configuration();
//		parameters.setBoolean("recursive.file.enumeration", true);
//		DataSource<String> readTextFile = env.readTextFile("file:\\D:\\input").withParameters(parameters);
//		readTextFile.print();
		
//		readTextFile.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
		
//		 DataSource<Tuple2<String, String>> types = env.readCsvFile("file:\\D:\\input")
//		    .includeFields("101").ignoreFirstLine().types(String.class,String.class);

		
//		readTextFile.first(4).print();
		
//		readTextFile.map(i -> Integer.parseInt(i) + 1).filter((i) -> i > 3).print();
		
//		readTextFile.mapPartition((Iterable<String> i, Collector<String> c) -> {
//			System.out.println("...");
//		}).returns(Types.STRING).print();
		
//		readTextFile.flatMap((String i, Collector<Integer> collector) -> {
//			collector.collect(Integer.parseInt(i) + 1);
//		}).returns(Types.INT).print();
//		
//		DataSet<Bean> pojoType = env.readCsvFile("file:\\D:\\input").ignoreFirstLine().pojoType(Bean.class, "name","sex"); //		pojoType.map(( i )-> i.getSex());

//		pojoType.map((i)-> Tuple2.of(i.getName(), i.getSex()))
//		        .returns(Types.TUPLE(Types.STRING, Types.STRING))
//		        .print();
		
		//DataSource<String> readTextFile = env.readTextFile("file:\\D:\\input");
//		readTextFile.flatMap((String i, Collector<Tuple2<String, Integer>> collector) -> {
//			String[] split = i.split(" ");
//			for (String string : split) {
//				collector.collect(new Tuple2<String, Integer>(string, 1));
//			}
//		}).returns(Types.TUPLE(Types.STRING, Types.INT)).groupBy(0).sum(1).setParallelism(3).print();
	}
}
