package com.hazeltask.hazelcast;

import static com.hazelcast.nio.SerializationHelper.readObject;
import static com.hazelcast.nio.SerializationHelper.writeObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.hazelcast.nio.DataSerializable;


public class FunctionSerializer {
	private enum SerializationMethod { ANON, FULL }
	
	private final ClassLoader loader;
	
	public FunctionSerializer() {
		loader = Thread.currentThread().getContextClassLoader();
	}
	
	public void write(Function<?, ?> function, DataOutput stream) throws IOException {
		if(function instanceof Serializable || function instanceof DataSerializable) {
			stream.writeInt(SerializationMethod.FULL.ordinal());
			writeObject(stream, function);
			
		} else if(function.getClass().isAnonymousClass()) {
			stream.writeInt(SerializationMethod.ANON.ordinal());
			serializeAnon(function, stream);
			
		} else {
			throw new IllegalArgumentException("Function must be Serializable, DataSerializable, or anonymous with no enclosing class references");
		}
	}
	
	protected void serializeAnon(Function<?, ?> function, DataOutput stream) throws IOException {
		Class<?> cls = function.getClass();
		stream.writeUTF(cls.getName());
		
		Field[] fields = cls.getDeclaredFields();
		stream.writeInt(fields.length);
		for(int i=0; i<fields.length; i++) {
			Field field = fields[i];
			field.setAccessible(true);

			try {
				if(field.getType().equals(cls.getEnclosingMethod().getDeclaringClass())) {
					stream.writeBoolean(true);
					writeObject(stream, null);
					
				} else if(Function.class.isAssignableFrom(field.getType())) {
					stream.writeBoolean(false);
					write((Function<?, ?>) field.get(function), stream);
					
				} else {
					stream.writeBoolean(true);
					writeObject(stream, field.get(function));
				}
				
			} catch(Exception e) {
				throw Throwables.propagate(e);
			}
		}
	}
	
	public <T, U> Function<T, U> read(DataInput stream) throws IOException, ClassNotFoundException {
		int method = stream.readInt();
		if(method >= SerializationMethod.values().length)
			throw new IOException("Unkown serialization type for Function: " + method);
		
		switch(SerializationMethod.values()[method]) {
			case FULL: return deserialize(stream);
			case ANON: return deserializeAnon(stream); 
				
			default: throw new IllegalStateException("Unimplemented serialization type recieved");
		}
	}
	
	@SuppressWarnings("unchecked")
	protected <T, U> Function<T, U> deserialize(DataInput stream) throws IOException {
		return (Function<T, U>) readObject(stream);
	}
	
	protected <T, U> Function<T, U> deserializeAnon(DataInput stream) throws IOException, ClassNotFoundException {
		String clsName = stream.readUTF();
		int argCount = stream.readInt();
		Object[] args=  new Object[argCount];
		
		for(int i=0; i<argCount; i++) {
			if(stream.readBoolean()) {
				args[i] = readObject(stream);
			} else {
				args[i] = read(stream);
			}
		}
			
		return initAnon(clsName, args);
	}
	
	@SuppressWarnings("unchecked")
	protected <T, U> Function<T, U> initAnon(String name, Object[] args) throws ClassNotFoundException {
		Class<Function<T, U>> fnClass = (Class<Function<T, U>>) loader.loadClass(name);
		
		Constructor<Function<T, U>> constructor = (Constructor<Function<T, U>>) fnClass.getDeclaredConstructors()[0];
		try {
			constructor.setAccessible(true);
			return (Function<T, U>) constructor.newInstance(args);
			
		} catch(Exception e) {
			throw new IllegalStateException("Unable to instantiate function class", e);
		}
	}
}
