/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.example;

public final class JobSelector
{
    private JobSelector()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static void main(String[] args)
    {
        String jobClassName = "SampleCassandraJob";
        if (args.length != 1)
        {
            System.out.println("Invalid number of arguments supplied. Fall back to run SampleCassandraJob");
        }
        else
        {
            jobClassName = args[0];
        }

        if (jobClassName.equalsIgnoreCase(SampleCassandraJob.class.getSimpleName()))
        {
            SampleCassandraJob.main(args);
        }
        else if (jobClassName.equalsIgnoreCase(LocalS3CassandraWriteJob.class.getSimpleName()))
        {
            // shift by 1
            String[] newArgs = new String[args.length - 1];
            System.arraycopy(args, 1, newArgs, 0, newArgs.length);
            LocalS3CassandraWriteJob.main(newArgs);
        }
        else
        {
            System.err.println("Unknown job class named supplied. ClassName: " + jobClassName);
        }
    }
}
