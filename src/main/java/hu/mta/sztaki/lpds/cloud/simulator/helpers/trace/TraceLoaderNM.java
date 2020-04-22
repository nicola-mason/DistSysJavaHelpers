package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace;

import java.lang.reflect.InvocationTargetException;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file.TraceFileReaderFoundation;

public class TraceLoaderNM extends TraceFileReaderFoundation{

	public TraceLoaderNM(String fileName, int from, int to, boolean allowedReadingFurther,
			Class<? extends Job> jobType) throws SecurityException, NoSuchMethodException {
		super("Log format", fileName, from, to, allowedReadingFurther, jobType);
	}
	
	@Override
	protected boolean isTraceLine(String traceLine) {
		String[] split = traceLine.split(" ");
		// validate line - 
		if(split[0].equals(Integer.parseInt(split[0]))
			&& split[1].equals(Float.parseFloat(split[1]))
			&& !split[2].contains(" ")
			&& (split[3].contains("url") || split[3].contains("default") || split[3].contains("export")))
		{
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	protected void metaDataCollector(String traceLine) {
		
	}
	
	@Override
	protected Job createJobFromLine(String traceLine) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		String[] split = traceLine.split(" ");
		
		//job arrival time and duration - round into whole seconds
		int arrTimeRounded = Math.round(Float.parseFloat(split[0]));
		int durationRounded = Math.round(Float.parseFloat(split[1]));
		
		try {
			String id = split[2];
			long submit = Long.valueOf(arrTimeRounded);
			long queue = 0;
			long exec = Long.valueOf(durationRounded);
			int nprocs = 1;
			double ppCpu = 1;
			long ppMem = 512;
			String user = null;
			String group = null;
			String executable = split[3];
			Job preceding = null;
			long delayAfter = 0;
			
			return jobCreator.newInstance(id, submit, queue, exec, nprocs, ppCpu, ppMem, user, group, executable, preceding, delayAfter);
			
		} catch (ArrayIndexOutOfBoundsException e) {
			return null;
		}
	}
}
