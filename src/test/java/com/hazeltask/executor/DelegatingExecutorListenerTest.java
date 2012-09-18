package com.hazeltask.executor;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class DelegatingExecutorListenerTest {
    
    private DelegatingExecutorListener listener;
    private ExecutorListener listener1;
    private ExecutorListener listener2;
    private HazelcastWork task;
    
    @Before
    public void setup() {
        listener1 = mock(ExecutorListener.class);
        listener2 = mock(ExecutorListener.class);
        task = new HazelcastWork("test", new WorkId("1","1"), (Runnable) null);
        listener = new DelegatingExecutorListener(Arrays.asList(listener1, listener2));
    }
    
    @Test
    public void testBeforeExecute() {
        when(listener1.beforeExecute(task)).thenReturn(true);
        when(listener2.beforeExecute(task)).thenReturn(true);
        
        listener.beforeExecute(task);
        
        verify(listener1).beforeExecute(eq(task));
        verify(listener2).beforeExecute(eq(task));
    }
    
    @Test
    public void testBeforeExecute2() {
        when(listener1.beforeExecute(task)).thenReturn(false);
        when(listener2.beforeExecute(task)).thenReturn(true);
        
        listener.beforeExecute(task);
        
        verify(listener1).beforeExecute(eq(task));
        verify(listener2).beforeExecute(eq(task));
    }
    
    @Test
    public void testAfterExecuteSuccess() {
        listener.afterExecute(task, null);
        verify(listener1).afterExecute(eq(task), (Throwable)isNull());
        verify(listener2).afterExecute(eq(task), (Throwable)isNull());
    }
    
    @Test
    public void testAfterExecuteException() {
        RuntimeException ex = new RuntimeException();
        listener.afterExecute(task, ex);
        verify(listener1).afterExecute(eq(task), eq(ex));
        verify(listener2).afterExecute(eq(task), eq(ex));
    }
    
    //TODO: test when listeners throw an exception
}