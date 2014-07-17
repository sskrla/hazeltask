package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.hazeltask.executor.TaskQuery;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.hazelcast.FunctionSerializer;

public class QueryOp<R, GROUP extends Serializable> extends AbstractClusterOp<R, GROUP> {
	private static final long serialVersionUID = 1L;
	
	private static final FunctionSerializer serializer = new FunctionSerializer();
	
	private Function<Iterator<HazeltaskTask<GROUP>>, R> query;
	
	private QueryOp(){super(null);}
	
	public QueryOp(String topology, TaskQuery<R, GROUP> query) {
		super(topology);
		this.query = query;
	}
	
	@Override
	public R call() throws Exception {
		Iterator<HazeltaskTask<GROUP>> it = getDistributedExecutorService()
			.getLocalTaskExecutorService()
			.getQueueIterator();
		
		return query.apply(it);
	}

	@Override
	protected void readChildData(DataInput in) throws IOException {
		try {
			query = serializer.read(in);
		} catch(ClassNotFoundException e) {
			Throwables.propagate(e);
		}
	}

	@Override
	protected void writChildData(DataOutput out) throws IOException {
		serializer.write(query, out);
	}
}
