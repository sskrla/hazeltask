package com.succinctllc.executor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.hazelcast.partition.Partition;

public class DistributedFuture<V> implements Future<V>, WorkKeyable {

	private final WorkKey key;
	private final DistributedWorkTopology topology;
	
    public DistributedFuture(DistributedWorkTopology topology, WorkKey key) {
        this.key = key;
        this.topology = topology;
    }
    
    private Partition getHazelcastPartition() {
    	return topology.hazelcast.getPartitionService().getPartition(key.getPartitionKey());
    }
    
    public DistributedFuture(DistributedWorkTopology topology, WorkKey key, V result) {
    	this.key = key;
    	this.topology = topology;
    }

	public boolean cancel(boolean mayInterruptIfRunning) {
		throw new RuntimeException("Not implemented yet");
	}

	public V get() throws InterruptedException, ExecutionException {
		/* add to this node's, notification-request map
		 * send a message to the member executing the work to let this node know when its done
		 * other member will keep a map of key->member for notifications
		 * 
		 * this method will wait()
		 * 
		 * when this node gets a response, it will look through its notification-request map and notify() 
		 * this future.
		 * 
		 */
		throw new RuntimeException("Not implemented yet");
	}

	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		//same as above but we will wait() with timeout
		throw new RuntimeException("Not implemented yet");
	}

	public boolean isCancelled() {
		throw new RuntimeException("Not implemented yet");
	}

	public boolean isDone() {
		throw new RuntimeException("Not implemented yet");
	}

	public WorkKey getKey() {
		return key;
	}

}
