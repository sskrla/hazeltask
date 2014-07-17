package com.hazeltask.hazelcast;

import java.io.IOException;

import junit.framework.Assert;
import lombok.SneakyThrows;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

public class FunctionSerializerTest {
	private final FunctionSerializer serializer = new FunctionSerializer();
	
	@Test
	public void simple() throws IOException, ClassNotFoundException {
		Assert.assertEquals(10,
			(int) passOverWire(new Function<String, Integer>(){
				@Override
				public Integer apply(String input) {
					return Integer.parseInt(input);
				}
			}).apply("10"));
	}
	
	@Test
	public void nested() {
		Assert.assertEquals(10, 
			(int) new Function<String, Integer>() {
				@Override
				@SneakyThrows
				public Integer apply(String input) {
					return passOverWire(new Function<String, Integer>(){
						@Override
						public Integer apply(String input) {
							return Integer.parseInt(input);
						}
					}).apply(input);
				}
			}.apply("10"));
	}
	
	@Test
	public void primitiveClosedVars() throws IOException, ClassNotFoundException {
		paramClosure(10, 10);
	}
	
	void paramClosure(final int adder, final int secondAdder) throws ClassNotFoundException, IOException {
		Assert.assertEquals(30,
			(int) passOverWire(new Function<String, Integer>(){
				@Override
				public Integer apply(String input) {
					return adder + secondAdder + Integer.parseInt(input);
				}
			}).apply("10"));
	}
	
	<T, U> Function<T, U> passOverWire(Function<T, U> function) throws IOException, ClassNotFoundException {
		ByteArrayDataOutput out = ByteStreams.newDataOutput();
		
		serializer.serializeAnon(function, out);
		
		ByteArrayDataInput in = ByteStreams.newDataInput(out.toByteArray());
		
		return serializer.deserializeAnon(in);
	}
}
