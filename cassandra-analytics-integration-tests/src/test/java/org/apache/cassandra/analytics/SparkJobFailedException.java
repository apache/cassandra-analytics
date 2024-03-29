/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics;

import java.util.List;

public class SparkJobFailedException extends RuntimeException
{
    private final List<String> command;
    private final int exitCode;
    private final String stdout;
    private final String stdErr;

    public SparkJobFailedException(String message, List<String> command, int exitCode, String stdout, String stdErr)
    {
        super(message);
        this.command = command;
        this.exitCode = exitCode;
        this.stdout = stdout;
        this.stdErr = stdErr;
    }

    public List<String> getCommand()
    {
        return command;
    }

    public int getExitCode()
    {
        return exitCode;
    }

    public String getStdout()
    {
        return stdout;
    }

    public String getStdErr()
    {
        return stdErr;
    }

    public String toString()
    {
        return "Failed to run Spark job: {" +
               "command=" + command +
               ", exitCode=" + exitCode +
               ", stdout='" + stdout + '\'' +
               ", stdErr='" + stdErr + '\'' +
               '}';
    }
}
