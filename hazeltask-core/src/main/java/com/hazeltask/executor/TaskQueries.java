package com.hazeltask.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.hazeltask.executor.task.HazeltaskTask;

public class TaskQueries {
	/**
	 * Iterates the tasks and invokes the provided <tt>transform</tt> on either the nested callable or runnable. If the transform
	 * returns an absent optional, that item will be ignored.
	 * 
	 * @param transform
	 * @return
	 */
	public static <G extends Serializable, R> TaskQuery<Collection<R>, G> forEachUnwrapped(final Function<?, Optional<R>> transform) {
		return new TaskQuery<Collection<R>, G>() {
			@Override
			public Collection<R> apply(Iterator<HazeltaskTask<G>> input) {
				List<R> result = Lists.newArrayList();
				
				while(input.hasNext()) {
					HazeltaskTask<?> task = input.next();
					
					@SuppressWarnings("unchecked")
					Optional<R> transformed = ((Function<Object, Optional<R>>) transform).apply(
						task.getInnerCallable() != null
							? task.getInnerCallable() 
							: task.getInnerRunnable());
					
					if(transformed.isPresent())
						result.add(transformed.get());
				}
				
				return result;
			}
		};
	}
}
