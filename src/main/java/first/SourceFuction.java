package first;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class SourceFuction implements ParallelSourceFunction<Long> {

	boolean isResult = true;
	long count = 1;

	@Override
	public void run(SourceContext ctx) throws Exception {
		while (isResult) {
			ctx.collect(count);
			count = count + 1;
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isResult = false;
	}

}
