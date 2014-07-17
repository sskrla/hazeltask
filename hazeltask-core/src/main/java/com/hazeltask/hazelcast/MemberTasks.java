package com.hazeltask.hazelcast;

import static com.hazelcast.nio.SerializationHelper.readObject;
import static com.hazelcast.nio.SerializationHelper.writeObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.Lists;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MultiTask;
import com.hazelcast.impl.InnerFutureTask;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.DefaultSerializer;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.SerializationHelper;

@Slf4j
public class MemberTasks {
    
	public static class MemberResponse<T> implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private T value;
        private Member member;
       
        public MemberResponse(){}
        
        public MemberResponse(Member member, T value) {
            this.value = value;
            this.member = member;
        }
        public T getValue() {
            return value;
        }
        public Member getMember() {
            return member;
        }
    }
    
    public static <T> MultiTask<T> create(Callable<T> callable, Set<Member> members) {
        return new MultiTask<T>(callable, members);
    }
    
    public static <T> DistributedTask<T> create(Callable<T> callable, Member member) {
        return new DistributedTask<T>(callable, member);
    }
    
    /**
     * Will wait a maximum of 1 minute for each node to response with their result.  If an error occurs on any
     * member, we will always attempt to continue execution and collect as many results as possible.
     * 
     * @param execSvc
     * @param members
     * @param callable
     * @return
     */
    public static <T> Collection<MemberResponse<T>> executeOptimistic(ExecutorService execSvc, Set<Member> members, Callable<T> callable) {
    	return executeOptimistic(execSvc, members, callable, 60, TimeUnit.SECONDS);
    }
    
    /**
     * We will always try to gather as many results as possible and never throw an exception.
     * 
     * TODO: Make MemberResponse hold an exception that we can populate if something bad happens so we always
     *       get to return something for a member in order to indicate a failure.  Getting the result when there
     *       is an error should throw an exception.
     * 
     * @param execSvc
     * @param members
     * @param callable
     * @param maxWaitTime - a value of 0 indicates forever
     * @param unit
     * @return
     */
    public static <T> Collection<MemberResponse<T>> executeOptimistic(ExecutorService execSvc, Set<Member> members, Callable<T> callable, long maxWaitTime, TimeUnit unit) {
       
        Collection<MemberResponse<T>> result = new ArrayList<MemberResponse<T>>(members.size());
        Collection<DistributedTask<MemberResponse<T>>> futures = new ArrayList<DistributedTask<MemberResponse<T>>>(members.size());
        
        //we copy the member set because it could change under us and throw a NoSuchElementException
        for(Member m : Lists.newArrayList(members)) {
          	DistributedTask<MemberResponse<T>> futureTask = new DistributedTask<MemberResponse<T>>(new MemberResponseCallable<T>(callable, m), m);
            futures.add(futureTask);
            execSvc.execute(futureTask);
        }
        
        for(DistributedTask<MemberResponse<T>> future : futures) {
            try {
                if(maxWaitTime > 0)
                	result.add(future.get(maxWaitTime, unit));
                else
                	result.add(future.get());
                //ignore exceptions... return what you can
            } catch (InterruptedException e) {
            	Thread.currentThread().interrupt(); //restore interrupted status and return what we have
            	return result;
            } catch (MemberLeftException e) {
            	//ignore that this member left....
                //Member targetMember = getFutureInner(future).getMember();            	
            	//log.info("Unable to execute task on "+targetMember+". It has left the cluster.", e);
            } catch (ExecutionException e) {
            	if(e.getCause() instanceof InterruptedException) {
            	    //restore interrupted state and return
            	    Thread.currentThread().interrupt();
            	    return result;
            	} else {
            	    Member targetMember = getFutureInner(future).getMember();
            	    log.warn("Unable to execute task on "+targetMember+". There was an error.", e);
            	}
            } catch (TimeoutException e) {
            	Member targetMember = getFutureInner(future).getMember();
            	log.error("Unable to execute task on "+targetMember+" within 10 seconds.");
            } catch (RuntimeException e) {
            	Member targetMember = getFutureInner(future).getMember();
            	log.error("Unable to execute task on "+targetMember+". An unexpected error occurred.", e);
            }
        }
        
        return result;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private static <T> MemberResponseCallable<T> getFutureInner(DistributedTask<MemberResponse<T>> future) {
        Object o = future.getInner();
        if(o instanceof InnerFutureTask) {
            return (MemberResponseCallable<T>) ((InnerFutureTask) o).getCallable();
        } else if (o instanceof MemberResponseCallable) {
            return (MemberResponseCallable<T>) o;
        }
        return null;
    }
    
    public static class MemberResponseCallable<T> implements Callable<MemberResponse<T>>, DataSerializable {
        private static final long serialVersionUID = 1L;
        private Callable<T> delegate;
        private Member member;
        
        protected MemberResponseCallable() { }
        
        public MemberResponseCallable(Callable<T> delegate, Member member) {
            this.delegate = delegate;
            this.member = member;
        }
        
        public Member getMember() {
        	return this.member;
        }
        
        public Callable<T> getDelegate() {
            return delegate;
        }
        
        public MemberResponse<T> call() throws Exception {
            return new MemberResponse<T>(member, delegate.call());
        }

		@Override
		public void writeData(DataOutput out) throws IOException {
			writeObject(out, delegate);
			writeObject(out, member);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void readData(DataInput in) throws IOException {
			delegate = (Callable<T>) readObject(in);
			member   = (Member)      readObject(in);
		}        
    }
}
