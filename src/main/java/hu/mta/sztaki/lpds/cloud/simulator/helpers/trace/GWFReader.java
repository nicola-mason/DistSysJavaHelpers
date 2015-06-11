/*
 *  ========================================================================
 *  Helper classes to support simulations of large scale distributed systems
 *  ========================================================================
 *  
 *  This file is part of DistSysJavaHelpers.
 *  
 *    DistSysJavaHelpers is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *   DistSysJavaHelpers is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *  (C) Copyright 2012-2015, Gabor Kecskemeti (kecskemeti.gabor@sztaki.mta.hu)
 */

package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * An implementation of the generic trace file reader functionality to support
 * files from the grid workloads archive (http://gwa.ewi.tudelft.nl/).
 * 
 * @author 
 *         "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems, MTA SZTAKI (c) 2012-2015"
 */
public class GWFReader extends TraceFileReaderFoundation {

	/**
	 * Constructs a "gwf" file reader that later on can act as a trace producer
	 * for user side schedulers.
	 * 
	 * @param fileName
	 *            The full path to the gwf file that should act as the source of
	 *            the jobs produced by this trace producer.
	 * @param from
	 *            The first job in the gwf file that should be produced in the
	 *            job listing output.
	 * @param to
	 *            The last job in the gwf file that should be still in the job
	 *            listing output.
	 * @param allowReadingFurther
	 *            If true the previously listed "to" parameter is ignored if the
	 *            "getJobs" function is called on this trace producer.
	 * @param jobType
	 *            The class of the job implementation that needs to be produced
	 *            by this particular trace producer.
	 * @throws SecurityException
	 *             If the class of the jobType cannot be accessed by the
	 *             classloader of the caller.
	 * @throws NoSuchMethodException
	 *             If the class of the jobType does not hold one of the expected
	 *             constructors.
	 */
	public GWFReader(String fileName, int from, int to,
			boolean allowReadingFurther, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("GWF", fileName, from, to, allowReadingFurther, jobType);
	}

	/**
	 * Ensures the use of the textual constructor (of the Job class) with the
	 * "jobCreator".
	 */
	@Override
	protected Constructor<? extends Job> getCreator(Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		return jobType.getConstructor(String.class);
	}

	/**
	 * Determines if a particular line in the GWF file is representing a job
	 * 
	 * Actually ignores empty lines and lines starting with '#'
	 */
	@Override
	public boolean isTraceLine(final String line) {
		return basicTraceLineDetector("#", line);
	}

	/**
	 * Parses a single line of the tracefile and instantiates a job object out
	 * of it.
	 * 
	 * Uses the Job classes textual - Job(String) - constructor! This is ensured
	 * in the getCreator function as well.
	 */
	@Override
	public Job createJobFromLine(String line) throws IllegalArgumentException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException {
		return jobCreator.newInstance(line);
	}
}