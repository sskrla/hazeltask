package com.hazeltask.executor;

import java.io.Serializable;
import java.util.Iterator;

import com.google.common.base.Function;
import com.hazeltask.executor.task.HazeltaskTask;

public interface TaskQuery<R, GROUP extends Serializable> extends Function<Iterator<HazeltaskTask<GROUP>>, R> {

}
